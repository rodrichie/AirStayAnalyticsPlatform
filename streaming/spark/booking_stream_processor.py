"""
Booking Events Stream Processor
Processes booking events in real-time and updates availability
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

from streaming.spark.spark_config import (
    create_spark_session,
    BOOKING_EVENT_SCHEMA
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BookingStreamProcessor:
    """Process booking events stream"""
    
    def __init__(self, spark: SparkSession = None):
        """Initialize processor"""
        self.spark = spark or create_spark_session("Booking Stream Processor")
        
        # Database configuration
        self.db_config = {
            'url': 'jdbc:postgresql://postgres:5432/airstay_db',
            'user': 'airstay',
            'password': 'airstay_pass',
            'driver': 'org.postgresql.Driver'
        }
    
    def read_booking_stream(self):
        """Read booking events from Kafka"""
        logger.info("📖 Reading booking events stream...")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "booking-events") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON events
        events_df = df.select(
            col("key").cast("string").alias("partition_key"),
            from_json(
                col("value").cast("string"),
                BOOKING_EVENT_SCHEMA
            ).alias("event"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        # Flatten structure
        parsed_df = events_df.select(
            col("event.event_id"),
            col("event.event_type"),
            col("event.event_timestamp"),
            col("event.property_id"),
            col("event.user_id"),
            col("event.payload.*"),
            col("kafka_timestamp")
        )
        
        logger.info("✅ Booking stream configured")
        
        return parsed_df
    
    def process_booking_confirmations(self, stream_df):
        """Process confirmed bookings and update availability"""
        
        # Filter confirmed bookings
        confirmed_bookings = stream_df.filter(
            col("event_type") == "booking.confirmed"
        )
        
        # Write to PostgreSQL
        def write_booking_to_db(batch_df, batch_id):
            """Write batch to database"""
            logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")
            
            try:
                # Insert booking records
                batch_df.select(
                    col("booking_id"),
                    col("property_id"),
                    col("guest_id"),
                    col("check_in_date").cast("date"),
                    col("check_out_date").cast("date"),
                    col("nights"),
                    col("num_guests"),
                    col("base_price"),
                    col("cleaning_fee"),
                    col("service_fee"),
                    col("total_price"),
                    col("booking_status"),
                    col("booking_channel"),
                    col("event_timestamp").alias("created_at")
                ).write \
                    .format("jdbc") \
                    .option("url", self.db_config['url']) \
                    .option("dbtable", "silver.bookings") \
                    .option("user", self.db_config['user']) \
                    .option("password", self.db_config['password']) \
                    .option("driver", self.db_config['driver']) \
                    .mode("append") \
                    .save()
                
                logger.info(f"✅ Batch {batch_id} written to database")
                
            except Exception as e:
                logger.error(f"❌ Error writing batch {batch_id}: {e}")
        
        # Start stream
        query = confirmed_bookings \
            .writeStream \
            .foreachBatch(write_booking_to_db) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoints/bookings") \
            .start()
        
        return query
    
    def calculate_real_time_metrics(self, stream_df):
        """Calculate real-time booking metrics"""
        
        # Aggregate metrics by property (5-minute windows)
        metrics_df = stream_df \
            .withWatermark("event_timestamp", "10 minutes") \
            .groupBy(
                window("event_timestamp", "5 minutes"),
                "property_id",
                "booking_status"
            ) \
            .agg(
                count("booking_id").alias("booking_count"),
                sum("total_price").alias("total_revenue"),
                avg("total_price").alias("avg_booking_value"),
                sum("nights").alias("total_nights")
            )
        
        # Write to console for monitoring
        query = metrics_df \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .option("checkpointLocation", "/tmp/checkpoints/metrics") \
            .start()
        
        return query
    
    def detect_booking_anomalies(self, stream_df):
        """Detect anomalous booking patterns"""
        
        # Calculate statistics
        with_stats = stream_df \
            .withWatermark("event_timestamp", "1 hour") \
            .groupBy(
                window("event_timestamp", "10 minutes"),
                "property_id"
            ) \
            .agg(
                count("booking_id").alias("booking_count"),
                avg("total_price").alias("avg_price"),
                stddev("total_price").alias("stddev_price")
            )
        
        # Detect anomalies (e.g., unusual number of bookings)
        anomalies = with_stats.filter(
            (col("booking_count") > 10) |  # Too many bookings
            (col("stddev_price") > col("avg_price") * 0.5)  # High price variance
        )
        
        # Write anomalies to console
        query = anomalies \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .option("checkpointLocation", "/tmp/checkpoints/anomalies") \
            .start()
        
        return query
    
    def run(self):
        """Run all streaming queries"""
        logger.info("🚀 Starting booking stream processor...")
        
        # Read stream
        booking_stream = self.read_booking_stream()
        
        # Start processing queries
        queries = [
            self.process_booking_confirmations(booking_stream),
            self.calculate_real_time_metrics(booking_stream),
            self.detect_booking_anomalies(booking_stream)
        ]
        
        logger.info(f"✅ Started {len(queries)} streaming queries")
        
        # Wait for termination
        for query in queries:
            query.awaitTermination()


# Example usage
if __name__ == "__main__":
    processor = BookingStreamProcessor()
    processor.run()