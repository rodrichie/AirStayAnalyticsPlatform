"""
Availability Cache Sync DAG
Periodically syncs database availability to Redis cache
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'availability_cache_sync',
    default_args=default_args,
    description='Sync availability data to Redis cache',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    tags=['cache', 'availability', 'redis'],
)

def sync_active_properties(**context):
    """Sync availability for all active properties"""
    import psycopg2
    from streaming.processors.availability_cache_manager import AvailabilityCacheManager
    
    # Get list of active properties
    conn = psycopg2.connect(
        host="postgres",
        database="airstay_db",
        user="airstay",
        password="airstay_pass"
    )
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT property_id 
        FROM silver.properties 
        WHERE is_active = TRUE
        ORDER BY property_id
    """)
    
    property_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    # Sync each property
    manager = AvailabilityCacheManager()
    
    total_synced = 0
    for property_id in property_ids:
        try:
            count = manager.sync_from_database(property_id, days_ahead=90)
            total_synced += count
        except Exception as e:
            logger.error(f"Error syncing property {property_id}: {e}")
    
    logger.info(f"✅ Synced {total_synced} records for {len(property_ids)} properties")
    
    return total_synced

def cleanup_expired_cache(**context):
    """Remove expired cache entries"""
    from streaming.processors.availability_cache_manager import AvailabilityCacheManager
    
    manager = AvailabilityCacheManager()
    
    # Redis handles TTL automatically, but we can clean up old calendars
    from datetime import date
    today = date.today()
    
    # This is handled by Redis TTL, but we log stats
    stats = manager.get_cache_stats()
    
    logger.info(f"📊 Cache stats: {stats}")
    
    return stats

with dag:
    sync_task = PythonOperator(
        task_id='sync_active_properties',
        python_callable=sync_active_properties,
        provide_context=True,
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_expired_cache',
        python_callable=cleanup_expired_cache,
        provide_context=True,
    )
    
    sync_task >> cleanup_task