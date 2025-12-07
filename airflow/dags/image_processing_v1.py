"""
Image Processing Pipeline
Processes property images: quality scoring, classification, thumbnails
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
    'image_processing_v1',
    default_args=default_args,
    description='Process property images with quality scoring and classification',
    schedule_interval='@daily',
    catchup=False,
    tags=['batch', 'images', 'ml'],
)

def get_properties_needing_processing(**context):
    """Get properties with unprocessed images"""
    import psycopg2
    
    conn = psycopg2.connect(
        host="postgres",
        database="airstay_db",
        user="airstay",
        password="airstay_pass"
    )
    
    # Get properties where images haven't been processed
    # or were updated recently
    query = """
        SELECT 
            property_id,
            listing_id,
            property_images
        FROM silver.properties
        WHERE array_length(property_images, 1) > 0
        AND (
            updated_at >= CURRENT_DATE - INTERVAL '1 day'
            OR quality_score IS NULL
        )
        LIMIT 100
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    properties = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    property_list = [
        {
            'property_id': row[0],
            'listing_id': row[1],
            'image_urls': row[2]
        }
        for row in properties
    ]
    
    logger.info(f"Found {len(property_list)} properties to process")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='properties', value=property_list)
    
    return len(property_list)

def process_images_batch(**context):
    """Process images for all properties in batch"""
    from image_processor import PropertyImageProcessor
    
    properties = context['task_instance'].xcom_pull(
        task_ids='get_properties',
        key='properties'
    )
    
    processor = PropertyImageProcessor()
    
    processed_results = []
    
    for prop in properties:
        try:
            results = processor.process_property_images(
                property_id=prop['property_id'],
                image_urls=prop['image_urls']
            )
            
            processed_results.append({
                'property_id': prop['property_id'],
                'avg_quality': results['avg_quality_score'],
                'room_types': results['room_types'],
                'has_exterior': results['has_exterior'],
                'processed_count': len(results['processed_images'])
            })
            
        except Exception as e:
            logger.error(f"Error processing property {prop['property_id']}: {e}")
            continue
    
    # Push results to XCom
    context['task_instance'].xcom_push(key='processed_results', value=processed_results)
    
    logger.info(f"✅ Processed images for {len(processed_results)} properties")
    
    return len(processed_results)

def update_property_metadata(**context):
    """Update property records with image processing results"""
    import psycopg2
    from psycopg2.extras import execute_values
    
    results = context['task_instance'].xcom_pull(
        task_ids='process_images',
        key='processed_results'
    )
    
    conn = psycopg2.connect(
        host="postgres",
        database="airstay_db",
        user="airstay",
        password="airstay_pass"
    )
    
    cursor = conn.cursor()
    
    # Update properties with image quality scores
    for result in results:
        # Update quality score based on image quality
        image_quality_contribution = result['avg_quality'] * 0.2
        
        # Bonus for having exterior photos
        exterior_bonus = 0.05 if result['has_exterior'] else 0.0
        
        # Update the property
        update_query = """
            UPDATE silver.properties
            SET quality_score = COALESCE(quality_score, 0) + %s + %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE property_id = %s
        """
        
        cursor.execute(
            update_query,
            (image_quality_contribution, exterior_bonus, result['property_id'])
        )
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"✅ Updated {len(results)} property records")
    
    return len(results)

with dag:
    # Task 1: Get properties needing processing
    get_properties = PythonOperator(
        task_id='get_properties',
        python_callable=get_properties_needing_processing,
        provide_context=True,
    )
    
    # Task 2: Process images
    process_images = PythonOperator(
        task_id='process_images',
        python_callable=process_images_batch,
        provide_context=True,
    )
    
    # Task 3: Update property metadata
    update_metadata = PythonOperator(
        task_id='update_metadata',
        python_callable=update_property_metadata,
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
                'image_processing_v1',
                '{{ ds }}',
                'SUCCESS',
                {{ task_instance.xcom_pull(task_ids='process_images') }}
            );
        """,
    )
    
    # Dependencies
    get_properties >> process_images >> update_metadata >> record_run