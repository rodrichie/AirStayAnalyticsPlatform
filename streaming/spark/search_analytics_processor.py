"""
Search Analytics Stream Processor
Analyzes search patterns in real-time  to detect trends and optimize recommendations
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging 

from streaming.spark.spark_config import (
    create_spark_session,
    SEARCH_EVENT_SCHEMA
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SearchAnalyticsProcessor:
    """Process search events for real-time analytics"""

    def __init__(self, spark: SparkSession = None):
        """initialize processor"""
        self.spark = spark or create_spark_session("Search Analytics Processor")

        self.db_config = {
            'url': 'jdbc:postgresql://postgres:5432/airstay_db',
            'user': 'airstay',
            'password': 'airstay_pass',
            'driver': 'org.postgresql.Driver'
        }

    def read_search_stream(self):
        """Read search events from Kafka"""
        logger.info("📖 Reading search events stream...")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "search-events") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON events
        events_df = df.select(
            col("key").cast("string").alias("session_id"),
            from_json(
                col("value").cast("string"),
                SEARCH_EVENT_SCHEMA
            ).alias("event"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        # Flatten structure
        parsed_df = events_df.select(
            col("event.event_id"),
            col("event.event_timestamp"),
            col("event.user_id"),
            col("event.session_id"),
            col("event.payload.search_id"),
            col("event.payload.city"),
            col("event.payload.check_in_date"),
            col("event.payload.check_out_date"),
            col("event.payload.num_guests"),
            col("event.payload.min_bedrooms"),
            col("event.payload.max_price"),
            col("event.payload.results_count"),
            col("event.payload.search_duration_ms")
        )
        
        return parsed_df    
    
    def calculate_popular_destinations(self, stream_df):
        """Calculate most searched destinations in real-time"""
        
        # Aggregate searches by city (10-minute windows)
        popular_cities = stream_df \
            .filter(col("city").isNotNull()) \
            .withWatermark("event_timestamp", "15 minutes") \
            .groupBy(
                window("event_timestamp", "10 minutes", "5 minutes"),
                "city"
            ) \
            .agg(
                count("search_id").alias("search_count"),
                countDistinct("user_id").alias("unique_users"),
                avg("results_count").alias("avg_results"),
                avg("search_duration_ms").alias("avg_duration_ms")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("city"),
                col("search_count"),
                col("unique_users"),
                round(col("avg_results"), 2).alias("avg_results"),
                round(col("avg_duration_ms"), 2).alias("avg_duration_ms")
            )
        
        # write to PostgreSQL
        def write_popular_cities(batch_df, batch_id):
            """Write batch to database"""
            logger.info(f"Writing popular cities batch {batch_id}")
            
            try:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", self.db_config['url']) \
                    .option("dbtable", "silver.search_trends_cities") \
                    .option("user", self.db_config['user']) \
                    .option("password", self.db_config['password']) \
                    .option("driver", self.db_config['driver']) \
                    .mode("append") \
                    .save()
                
                logger.info(f"✅ Batch {batch_id} written")
            except Exception as e:
                logger.error(f"Error writing batch: {e}")
        
        query = popular_cities \
            .writeStream \
            .foreachBatch(write_popular_cities) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoints/popular_cities") \
            .start()
        
        return query
    
    def detect_search_patterns(self, stream_df):
        """Detect search patterns and trends"""
        
        # Calculate search filters usage
        filter_usage = stream_df \
            .withWatermark("event_timestamp", "15 minutes") \
            .groupBy(
                window("event_timestamp", "15 minutes")
            ) \
            .agg(
                count("*").alias("total_searches"),
                count(when(col("num_guests").isNotNull(), 1)).alias("with_guests_filter"),
                count(when(col("min_bedrooms").isNotNull(), 1)).alias("with_bedrooms_filter"),
                count(when(col("max_price").isNotNull(), 1)).alias("with_price_filter"),
                avg("num_guests").alias("avg_guests"),
                percentile_approx("max_price", 0.5).alias("median_price_filter"),
                avg("results_count").alias("avg_results")
            )
        
        # Console output for monitoring
        query = filter_usage \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("checkpointLocation", "/tmp/checkpoints/filter_usage") \
            .start()
        
        return query
    
    def calculate_conversion_funnel(self, stream_df):
        """
        Calculate search-to-booking conversion metrics
        This would typically join with user-activity stream
        """
        
        # Session-level aggregations
        session_metrics = stream_df \
            .withWatermark("event_timestamp", "30 minutes") \
            .groupBy(
                "session_id",
                window("event_timestamp", "30 minutes")
            ) \
            .agg(
                count("search_id").alias("searches_in_session"),
                first("city").alias("primary_city"),
                avg("results_count").alias("avg_results"),
                min("event_timestamp").alias("session_start"),
                max("event_timestamp").alias("session_end")
            ) \
            .withColumn(
                "session_duration_minutes",
                (unix_timestamp("session_end") - unix_timestamp("session_start")) / 60.0
            )
        
        # Console output
        query = session_metrics \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("checkpointLocation", "/tmp/checkpoints/session_metrics") \
            .start()
        
        return query
    
    def identify_zero_result_searches(self, stream_df):
        """Identify searches with no results"""
        
        zero_results = stream_df \
            .filter(col("results_count") == 0) \
            .select(
                "event_timestamp",
                "city",
                "check_in_date",
                "check_out_date",
                "num_guests",
                "min_bedrooms",
                "max_price"
            )
        
        # Aggregate zero-result searches
        zero_result_aggregates = zero_results \
            .withWatermark("event_timestamp", "1 hour") \
            .groupBy(
                window("event_timestamp", "1 hour"),
                "city"
            ) \
            .agg(
                count("*").alias("zero_result_count"),
                collect_list(
                    struct(
                        "num_guests",
                        "min_bedrooms",
                        "max_price"
                    )
                ).alias("failed_searches")
            )
        
        # Write to console for alerting
        query = zero_result_aggregates \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("checkpointLocation", "/tmp/checkpoints/zero_results") \
            .start()
        
        return query
    
    def calculate_date_range_preferences(self, stream_df):
        """Analyze preferred booking date ranges"""
        
        # Calculate days in advance and trip length
        searches_with_metrics = stream_df \
            .filter(
                col("check_in_date").isNotNull() &
                col("check_out_date").isNotNull()
            ) \
            .withColumn(
                "check_in_dt",
                to_date(col("check_in_date"))
            ) \
            .withColumn(
                "check_out_dt",
                to_date(col("check_out_date"))
            ) \
            .withColumn(
                "trip_length_days",
                datediff(col("check_out_dt"), col("check_in_dt"))
            ) \
            .withColumn(
                "days_in_advance",
                datediff(col("check_in_dt"), current_date())
            )
        
        # Aggregate statistics
        date_metrics = searches_with_metrics \
            .withWatermark("event_timestamp", "1 hour") \
            .groupBy(
                window("event_timestamp", "1 hour"),
                "city"
            ) \
            .agg(
                avg("trip_length_days").alias("avg_trip_length"),
                percentile_approx("trip_length_days", 0.5).alias("median_trip_length"),
                avg("days_in_advance").alias("avg_lead_time"),
                percentile_approx("days_in_advance", 0.5).alias("median_lead_time"),
                count("*").alias("search_count")
            )
        
        # Console output
        query = date_metrics \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("checkpointLocation", "/tmp/checkpoints/date_metrics") \
            .start()
        
        return query
    
    def run(self):
        """Run all streaming queries"""
        logger.info(" Starting search analytics processor...")

        # Read stream
        search_stream = self.read_search_stream()

        # start processing queries
        queries = [
            self.calculate_popular_destinations(search_stream),
            self.detect_search_patterns(search_stream),
            self.calculate_conversion_funnel(search_stream),
            self.identify_zero_result_searches(search_stream),
            self.calculate_date_range_preferences(search_stream)
        ]

        logger.info(f"Started {len(queries)} streaming queries")

        # Wait for termination
        for query in queries:
            query.awaitTermination()

# Example usage
if __name__ == "__main__":
    processor = SearchAnalyticsProcessor()
    processor.run()            