"""
Property Ingestion Pipeline
Processes property listings from raw data to Silver layer
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
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'property_ingestion_v1',
    default_args=default_args,
    description='Ingest and process property listings',
    schedule_interval='@daily',
    catchup=False,
    tags=['batch', 'properties', 'ingestion'],
)

def extract_new_listings(**context):
    """Extract new property listings"""
    import psycopg2
    import os
    
    conn = psycopg2.connect(
        host="postgres",
        database="airstay_db",
        user="airstay",
        password="airstay_pass"
    )
    
    # Get properties added/updated in last 24 hours
    query = """
        SELECT property_id, listing_id
        FROM silver.properties
        WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
        OR created_at >= CURRENT_DATE - INTERVAL '1 day'
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    properties = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    print(f"Found {len(properties)} properties to process")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='property_count', value=len(properties))
    
    return len(properties)

def geocode_addresses(**context):
    """Geocode property addresses (if needed)"""
    print("Geocoding addresses...")
    # In production, integrate with Google Maps Geocoding API
    return True

def calculate_quality_scores(**context):
    """Calculate property quality scores"""
    import psycopg2
    
    conn = psycopg2.connect(
        host="postgres",
        database="airstay_db",
        user="airstay",
        password="airstay_pass"
    )
    
    # Update quality scores based on:
    # - Number of amenities
    # - Image count
    # - Description length
    # - Response time
    # - Review ratings
    
    update_query = """
        UPDATE silver.properties
        SET quality_score = (
            (CASE WHEN array_length(amenities, 1) > 10 THEN 0.3 ELSE array_length(amenities, 1) * 0.03 END) +
            (CASE WHEN array_length(property_images, 1) > 5 THEN 0.2 ELSE array_length(property_images, 1) * 0.04 END) +
            (CASE WHEN LENGTH(description) > 200 THEN 0.2 ELSE LENGTH(description) / 1000.0 END) +
            (CASE WHEN response_time_minutes < 60 THEN 0.15 ELSE 0.05 END) +
            (CASE WHEN property_rating >= 4.5 THEN 0.15 ELSE property_rating * 0.03 END)
        ),
        updated_at = CURRENT_TIMESTAMP
        WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
    """
    
    cursor = conn.cursor()
    cursor.execute(update_query)
    rows_updated = cursor.rowcount
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Updated quality scores for {rows_updated} properties")
    return rows_updated

def update_search_index(**context):
    """Update Elasticsearch index for property search"""
    # In production, index properties in Elasticsearch
    print("Search index updated")
    return True

with dag:
    # Task 1: Extract new listings
    extract_task = PythonOperator(
        task_id='extract_new_listings',
        python_callable=extract_new_listings,
        provide_context=True,
    )
    
    # Task 2: Geocode addresses
    geocode_task = PythonOperator(
        task_id='geocode_addresses',
        python_callable=geocode_addresses,
        provide_context=True,
    )
    
    # Task 3: Calculate quality scores
    quality_task = PythonOperator(
        task_id='calculate_quality_scores',
        python_callable=calculate_quality_scores,
        provide_context=True,
    )
    
    # Task 4: Update search index
    search_task = PythonOperator(
        task_id='update_search_index',
        python_callable=update_search_index,
        provide_context=True,
    )
    
    # Task 5: Update metadata
    metadata_task = PostgresOperator(
        task_id='update_metadata',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO metadata.pipeline_runs (
                pipeline_name, run_date, status, records_processed
            ) VALUES (
                'property_ingestion_v1',
                '{{ ds }}',
                'SUCCESS',
                {{ task_instance.xcom_pull(task_ids='extract_new_listings', key='property_count') }}
            );
        """,
    )
    
    # Dependencies
    extract_task >> geocode_task >> quality_task >> search_task >> metadata_task