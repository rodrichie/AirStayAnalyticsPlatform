"""
Review Sentiment Analysis Pipeline
Analyzes review sentiment using NLP models
Updates review records with sentiment scores
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys
sys.path.append('/opt/airflow/scripts')

default_args = {
    'owner': 'airstay-de-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sentiment_analysis_v1',
    default_args=default_args,
    description='Analyze review sentiment with NLP',
    schedule_interval='@hourly',  # Run hourly to process new reviews
    catchup=False,
    tags=['batch', 'nlp', 'reviews'],
)

def process_new_reviews(**context):
    """Process reviews added in last hour"""
    from sentiment_analyzer import ReviewSentimentAnalyzer
    
    analyzer = ReviewSentimentAnalyzer()
    
    # Process up to 500 reviews per run
    processed_count = analyzer.process_reviews_batch(limit=500)
    
    logger.info(f"✅ Processed {processed_count} reviews")
    
    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='processed_count', value=processed_count)
    
    return processed_count

def update_property_ratings(**context):
    """Update property ratings based on recent reviews"""
    import psycopg2
    
    processed_count = context['task_instance'].xcom_pull(
        task_ids='process_reviews',
        key='processed_count'
    )
    
    if processed_count == 0:
        logger.info("No reviews processed, skipping rating update")
        return 0
    
    conn = psycopg2.connect(
        host="postgres",
        database="airstay_db",
        user="airstay",
        password="airstay_pass"
    )
    
    cursor = conn.cursor()
    
    # Recalculate property ratings including sentiment
    update_query = """
        WITH review_stats AS (
            SELECT 
                property_id,
                AVG(rating) as avg_rating,
                COUNT(*) as review_count,
                AVG(sentiment_score) as avg_sentiment
            FROM silver.reviews
            WHERE sentiment_score IS NOT NULL
            GROUP BY property_id
        )
        UPDATE silver.properties p
        SET 
            property_rating = rs.avg_rating,
            review_count = rs.review_count,
            updated_at = CURRENT_TIMESTAMP
        FROM review_stats rs
        WHERE p.property_id = rs.property_id
        AND rs.review_count > 0
    """
    
    cursor.execute(update_query)
    updated_count = cursor.rowcount
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"✅ Updated {updated_count} property ratings")
    
    return updated_count

def identify_trending_issues(**context):
    """Identify properties with declining sentiment"""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    conn = psycopg2.connect(
        host="postgres",
        database="airstay_db",
        user="airstay",
        password="airstay_pass"
    )
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Find properties with declining sentiment
    query = """
        WITH recent_sentiment AS (
            SELECT 
                property_id,
                AVG(sentiment_score) as recent_score,
                COUNT(*) as recent_count
            FROM silver.reviews
            WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
            AND sentiment_score IS NOT NULL
            GROUP BY property_id
            HAVING COUNT(*) >= 3
        ),
        overall_sentiment AS (
            SELECT 
                property_id,
                AVG(sentiment_score) as overall_score
            FROM silver.reviews
            WHERE sentiment_score IS NOT NULL
            GROUP BY property_id
        )
        SELECT 
            p.property_id,
            p.listing_id,
            p.title,
            rs.recent_score,
            os.overall_score,
            rs.recent_score - os.overall_score as sentiment_change
        FROM silver.properties p
        INNER JOIN recent_sentiment rs ON p.property_id = rs.property_id
        INNER JOIN overall_sentiment os ON p.property_id = os.property_id
        WHERE rs.recent_score - os.overall_score < -0.2
        ORDER BY sentiment_change
        LIMIT 20
    """
    
    cursor.execute(query)
    declining_properties = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    if declining_properties:
        logger.warning(f"⚠️ Found {len(declining_properties)} properties with declining sentiment")
        
        for prop in declining_properties:
            logger.warning(
                f"  Property {prop['listing_id']}: "
                f"sentiment declined from {prop['overall_score']:.2f} to {prop['recent_score']:.2f}"
            )
    
    return len(declining_properties)

with dag:
    # Task 1: Process new reviews
    process_reviews = PythonOperator(
        task_id='process_reviews',
        python_callable=process_new_reviews,
        provide_context=True,
    )
    
    # Task 2: Update property ratings
    update_ratings = PythonOperator(
        task_id='update_ratings',
        python_callable=update_property_ratings,
        provide_context=True,
    )
    
    # Task 3: Identify trending issues
    check_trends = PythonOperator(
        task_id='check_trends',
        python_callable=identify_trending_issues,
        provide_context=True,
    )
    
    # Task 4: Record pipeline run
    record_run = PostgresOperator(
        task_id='record_pipeline_run',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO metadata.pipeline_runs (
                pipeline_name, run_date, status, records_processed
            ) VALUES (
                'sentiment_analysis_v1',
                '{{ ds }}',
                'SUCCESS',
                {{ task_instance.xcom_pull(task_ids='process_reviews', key='processed_count') }}
            );
        """,
    )
    
    # Dependencies
    process_reviews >> update_ratings >> check_trends >> record_run