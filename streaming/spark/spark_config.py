"""
Spark Configuration for Structured Streaming
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "AirStay Streaming") -> SparkSession:
    """
    Create Spark session with streaming configurations
    
    Args:
        app_name: Application name
        
    Returns:
        Configured SparkSession
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"✅ Spark session created: {app_name}")
    logger.info(f"   Spark version: {spark.version}")
    logger.info(f"   Master: {spark.sparkContext.master}")
    
    return spark


# Event schemas for Spark
BOOKING_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("property_id", IntegerType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("payload", StructType([
        StructField("booking_id", IntegerType(), False),
        StructField("property_id", IntegerType(), False),
        StructField("guest_id", IntegerType(), False),
        StructField("host_id", IntegerType(), False),
        StructField("check_in_date", StringType(), False),
        StructField("check_out_date", StringType(), False),
        StructField("nights", IntegerType(), False),
        StructField("num_guests", IntegerType(), False),
        StructField("base_price", DoubleType(), False),
        StructField("cleaning_fee", DoubleType(), True),
        StructField("service_fee", DoubleType(), True),
        StructField("total_price", DoubleType(), False),
        StructField("booking_status", StringType(), False),
        StructField("booking_channel", StringType(), True),
    ])),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

REVIEW_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("property_id", IntegerType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("payload", StructType([
        StructField("review_id", IntegerType(), False),
        StructField("property_id", IntegerType(), False),
        StructField("booking_id", IntegerType(), False),
        StructField("guest_id", IntegerType(), False),
        StructField("rating", IntegerType(), False),
        StructField("cleanliness_rating", IntegerType(), True),
        StructField("accuracy_rating", IntegerType(), True),
        StructField("communication_rating", IntegerType(), True),
        StructField("location_rating", IntegerType(), True),
        StructField("value_rating", IntegerType(), True),
        StructField("review_text", StringType(), True),
        StructField("review_language", StringType(), True),
    ])),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

SEARCH_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("session_id", StringType(), False),
    StructField("payload", StructType([
        StructField("search_id", StringType(), False),
        StructField("city", StringType(), True),
        StructField("check_in_date", StringType(), True),
        StructField("check_out_date", StringType(), True),
        StructField("num_guests", IntegerType(), True),
        StructField("min_bedrooms", IntegerType(), True),
        StructField("max_price", DoubleType(), True),
        StructField("results_count", IntegerType(), True),
        StructField("search_duration_ms", IntegerType(), True),
    ])),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

AVAILABILITY_UPDATE_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("property_id", IntegerType(), False),
    StructField("payload", StructType([
        StructField("property_id", IntegerType(), False),
        StructField("date", StringType(), False),
        StructField("is_available", BooleanType(), False),
        StructField("price", DoubleType(), False),
        StructField("minimum_nights", IntegerType(), True),
        StructField("reason", StringType(), False),
    ])),
    StructField("metadata", MapType(StringType(), StringType()), True)
])