"""
Spark job for booking analytics aggregation.
Processes booking data and generates performance metrics.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def create_spark_session():
    """Create Spark session with PostgreSQL support."""
    return SparkSession.builder \
        .appName("AirStay Booking Analytics") \
        .config("spark.jars", "/opt/spark-jobs/postgresql-42.6.0.jar") \
        .getOrCreate()


def load_bookings(spark, jdbc_url, properties):
    """Load bookings from PostgreSQL."""
    return spark.read.jdbc(
        url=jdbc_url,
        table="silver.bookings",
        properties=properties
    )


def load_properties(spark, jdbc_url, properties):
    """Load properties from PostgreSQL."""
    return spark.read.jdbc(
        url=jdbc_url,
        table="silver.properties",
        properties=properties
    )


def compute_daily_metrics(bookings_df, properties_df):
    """Compute daily booking metrics per property."""
    active_bookings = bookings_df.filter(
        F.col("booking_status").isin("confirmed", "completed")
    )

    daily = active_bookings \
        .withColumn("metric_date", F.to_date("created_at")) \
        .groupBy("property_id", "metric_date") \
        .agg(
            F.count("*").alias("bookings_count"),
            F.sum("total_price").alias("revenue_total"),
            F.sum("nights").alias("nights_booked"),
        )

    result = daily.join(
        properties_df.select("property_id", "base_price", "property_rating"),
        on="property_id",
        how="left"
    ).withColumn(
        "occupancy_rate",
        F.round(F.col("nights_booked") / 30 * 100, 2)
    ).withColumn(
        "avg_nightly_rate", F.round(F.col("base_price"), 2)
    ).withColumn(
        "avg_rating", F.round(F.col("property_rating"), 2)
    )

    return result


def compute_city_metrics(bookings_df, properties_df):
    """Compute daily city-level metrics."""
    joined = bookings_df.join(
        properties_df.select("property_id", "location_city", "base_price"),
        on="property_id",
        how="inner"
    ).filter(
        F.col("booking_status").isin("confirmed", "completed")
    )

    return joined \
        .withColumn("metric_date", F.to_date("created_at")) \
        .groupBy("location_city", "metric_date") \
        .agg(
            F.countDistinct("property_id").alias("active_properties"),
            F.count("*").alias("total_bookings"),
            F.sum("total_price").alias("total_revenue"),
            F.round(F.avg("base_price"), 2).alias("avg_nightly_rate"),
        ) \
        .withColumnRenamed("location_city", "city")


if __name__ == "__main__":
    spark = create_spark_session()

    jdbc_url = "jdbc:postgresql://postgres:5432/airstay_db"
    db_props = {
        "user": "airstay",
        "password": "airstay_pass",
        "driver": "org.postgresql.Driver"
    }

    bookings = load_bookings(spark, jdbc_url, db_props)
    properties = load_properties(spark, jdbc_url, db_props)

    daily_metrics = compute_daily_metrics(bookings, properties)
    city_metrics = compute_city_metrics(bookings, properties)

    daily_metrics.select(
        "property_id", "metric_date", "bookings_count",
        "revenue_total", "nights_booked", "occupancy_rate",
        "avg_nightly_rate", "avg_rating"
    ).write.jdbc(
        url=jdbc_url,
        table="gold.agg_property_performance",
        mode="append",
        properties=db_props
    )

    city_metrics.write.jdbc(
        url=jdbc_url,
        table="gold.agg_city_metrics",
        mode="append",
        properties=db_props
    )

    print("Booking analytics job completed successfully.")
    spark.stop()
