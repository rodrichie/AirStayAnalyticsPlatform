"""
Review Sentiment Streaming Pipeline
Processes review events and performs real-time sentiment analysis
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

from streaming.spark.spark_config import (
    create_spark_session,
    REVIEW_EVENT_SCHEMA
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ReviewSentimentStreamProcessor:
    """Process review events with real-time sentiment analysis"""
    
    def __init__(self, spark: SparkSession = None):
        """Initialize processor"""
        self.spark = spark or create_spark_session("Review Sentiment Stream")
        
        self.db_config = {
            'url': 'jdbc:postgresql://postgres:5432/airstay_db',
            'user': 'airstay',
            'password': 'airstay_pass',
            'driver': 'org.postgresql.Driver'
        }
    
    def read_review_stream(self):
        """Read review events from Kafka"""
        logger.info("📖 Reading review events stream...")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "review-events") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON events
        events_df = df.select(
            from_json(
                col("value").cast("string"),
                REVIEW_EVENT_SCHEMA
            ).alias("event"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        # Flatten structure
        parsed_df = events_df.select(
            col("event.event_id"),
            col("event.event_timestamp"),
            col("event.property_id"),
            col("event.user_id"),
            col("event.payload.*")
        )
        
        return parsed_df
    
    def calculate_simple_sentiment(self, stream_df):
        """
        Calculate simple sentiment score based on rating
        (Placeholder for actual NLP model)
        """
        
        # Convert 1-5 rating to -1 to 1 sentiment scale
        with_sentiment = stream_df.withColumn(
            "sentiment_score",
            (col("rating") - 3.0) / 2.0
        ).withColumn(
            "sentiment_label",
            when(col("rating") >= 4, "positive")
            .when(col("rating") <= 2, "negative")
            .otherwise("neutral")
        )
        
        return with_sentiment
    
    def aggregate_property_sentiment(self, stream_df):
        """Aggregate sentiment by property"""
        
        property_sentiment = stream_df \
            .withWatermark("event_timestamp", "1 hour") \
            .groupBy(
                window("event_timestamp", "1 hour"),
                "property_id"
            ) \
            .agg(
                count("review_id").alias("review_count"),
                avg("rating").alias("avg_rating"),
                avg("sentiment_score").alias("avg_sentiment"),
                count(when(col("sentiment_label") == "positive", 1)).alias("positive_count"),
                count(when(col("sentiment_label") == "neutral", 1)).alias("neutral_count"),
                count(when(col("sentiment_label") == "negative", 1)).alias("negative_count"),
                avg("cleanliness_rating").alias("avg_cleanliness"),
                avg("accuracy_rating").alias("avg_accuracy"),
                avg("communication_rating").alias("avg_communication"),
                avg("location_rating").alias("avg_location"),
                avg("value_rating").alias("avg_value")
            )
        
        # Write to database
        def write_sentiment_aggregates(batch_df, batch_id):
            """Write batch to database"""
            logger.info(f"Writing sentiment aggregates batch {batch_id}")
            
            try:
                batch_df.select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    "property_id",
                    "review_count",
                    round("avg_rating", 2).alias("avg_rating"),
                    round("avg_sentiment", 3).alias("avg_sentiment"),
                    "positive_count",
                    "neutral_count",
                    "negative_count",
                    round("avg_cleanliness", 2).alias("avg_cleanliness"),
                    round("avg_accuracy", 2).alias("avg_accuracy"),
                    round("avg_communication", 2).alias("avg_communication"),
                    round("avg_location", 2).alias("avg_location"),
                    round("avg_value", 2).alias("avg_value")
                ).write \
                    .format("jdbc") \
                    .option("url", self.db_config['url']) \
                    .option("dbtable", "silver.review_sentiment_aggregates") \
                    .option("user", self.db_config['user']) \
                    .option("password", self.db_config['password']) \
                    .option("driver", self.db_config['driver']) \
                    .mode("append") \
                    .save()
                
                logger.info(f"✅ Batch {batch_id} written")
            except Exception as e:
                logger.error(f"Error writing batch: {e}")
        
        query = property_sentiment \
            .writeStream \
            .foreachBatch(write_sentiment_aggregates) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoints/sentiment_agg") \
            .start()
        
        return query
    
    def detect_sentiment_alerts(self, stream_df):
        """Detect properties with declining sentiment"""
        
        # Calculate rolling sentiment
        window_spec = Window.partitionBy("property_id").orderBy("event_timestamp").rowsBetween(-10, 0)
        
        with_rolling = stream_df.withColumn(
            "rolling_avg_rating",
            avg("rating").over(window_spec)
        ).withColumn(
            "rolling_sentiment",
            avg("sentiment_score").over(window_spec)
        )
        
        # Alert on negative trends
        alerts = with_rolling.filter(
            (col("rolling_avg_rating") < 3.5) |
            (col("rolling_sentiment") < -0.3)
        )
        
        # Console output for alerts
        query = alerts \
            .select(
                "event_timestamp",
                "property_id",
                "review_id",
                "rating",
                "rolling_avg_rating",
                "sentiment_label"
            ) \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("checkpointLocation", "/tmp/checkpoints/sentiment_alerts") \
            .start()
        
        return query
    
    def write_reviews_to_database(self, stream_df):
        """Write processed reviews to database"""
        
        def write_reviews(batch_df, batch_id):
            """Write batch to database"""
            logger.info(f"Writing reviews batch {batch_id} with {batch_df.count()} records")
            
            try:
                batch_df.select(
                    "review_id",
                    "property_id",
                    "booking_id",
                    "guest_id",
                    "rating",
                    "cleanliness_rating",
                    "accuracy_rating",
                    "communication_rating",
                    "location_rating",
                    "value_rating",
                    "review_text",
                    "review_language",
                    "sentiment_score",
                    "sentiment_label",
                    col("event_timestamp").alias("created_at")
                ).write \
                    .format("jdbc") \
                    .option("url", self.db_config['url']) \
                    .option("dbtable", "silver.reviews") \
                    .option("user", self.db_config['user']) \
                    .option("password", self.db_config['password']) \
                    .option("driver", self.db_config['driver']) \
                    .mode("append") \
                    .save()
                
                logger.info(f"✅ Batch {batch_id} written to database")
            except Exception as e:
                logger.error(f"Error writing batch: {e}")
        
        query = stream_df \
            .writeStream \
            .foreachBatch(write_reviews) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoints/reviews") \
            .start()
        
        return query
    
    def run(self):
        """Run all streaming queries"""
        logger.info("🚀 Starting review sentiment stream processor...")
        
        # Read stream
        review_stream = self.read_review_stream()
        
        # Add sentiment
        with_sentiment = self.calculate_simple_sentiment(review_stream)
        
        # Start processing queries
        queries = [
            self.aggregate_property_sentiment(with_sentiment),
            self.detect_sentiment_alerts(with_sentiment),
            self.write_reviews_to_database(with_sentiment)
        ]
        
        logger.info(f"✅ Started {len(queries)} streaming queries")
        
        # Wait for termination
        for query in queries:
            query.awaitTermination()


# Example usage
if __name__ == "__main__":
    processor = ReviewSentimentStreamProcessor()
    processor.run()