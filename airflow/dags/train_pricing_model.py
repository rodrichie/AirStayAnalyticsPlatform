"""
Pricing Model Training DAG
Retrains dynamic pricing model weekly with latest data
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys
sys.path.append('/opt/airflow/scripts')

default_args = {
    'owner': 'airstay-ml-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'train_pricing_model',
    default_args=default_args,
    description='Train and evaluate dynamic pricing model',
    schedule_interval='0 2 * * 0',  # Weekly on Sunday at 2 AM
    catchup=False,
    tags=['ml', 'pricing', 'training'],
)

def train_model(**context):
    """Train pricing model"""
    from ml.models.dynamic_pricing_model import DynamicPricingModel
    
    model = DynamicPricingModel()
    
    # Prepare data
    X_train, X_test, y_train, y_test = model.prepare_training_data(
        test_size=0.2
    )
    
    # Train with early stopping
    model.train(X_train, y_train, X_test, y_test)
    
    # Evaluate
    metrics = model.evaluate(X_test, y_test, save_plots=True)
    
    # Save model
    model.save_model('/opt/airflow/models/pricing_model_latest.pkl')
    
    # Push metrics to XCom
    context['task_instance'].xcom_push(key='metrics', value=metrics)
    
    logger.info(f"✅ Model trained with R²: {metrics['r2']}")
    
    return metrics

def generate_pricing_recommendations(**context):
    """Generate price recommendations for all active properties"""
    from ml.models.dynamic_pricing_model import DynamicPricingModel
    import psycopg2
    from psycopg2.extras import execute_values
    
    # Load trained model
    model = DynamicPricingModel()
    model.load_model('/opt/airflow/models/pricing_model_latest.pkl')
    
    # Get all active properties
    conn = psycopg2.connect(
        host="postgres",
        database="airstay_db",
        user="airstay",
        password="airstay_pass"
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT property_id FROM silver.properties WHERE is_active = TRUE")
    property_ids = [row[0] for row in cursor.fetchall()]
    
    # Batch predict
    predictions_df = model.batch_predict(property_ids)
    
    # Save recommendations to database
    recommendations = []
    for _, row in predictions_df.iterrows():
        if row['current_price'] and abs(row['price_change_pct']) > 5:  # Only if >5% change
            recommendations.append((
                row['property_id'],
                datetime.now().date(),
                row['current_price'],
                row['predicted_price'],
                row['price_change_pct'],
                'ml_model_recommendation',
                0.85  # confidence score
            ))
    
    if recommendations:
        insert_query = """
            INSERT INTO gold.pricing_recommendations 
            (property_id, recommendation_date, current_price, recommended_price, 
             price_change_pct, reason, confidence_score)
            VALUES %s
            ON CONFLICT (property_id, recommendation_date) DO UPDATE SET
                recommended_price = EXCLUDED.recommended_price,
                price_change_pct = EXCLUDED.price_change_pct,
                confidence_score = EXCLUDED.confidence_score
        """
        
        execute_values(cursor, insert_query, recommendations)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"✅ Generated {len(recommendations)} pricing recommendations")
    
    return len(recommendations)

with dag:
    train_task = PythonOperator(
        task_id='train_pricing_model',
        python_callable=train_model,
        provide_context=True,
    )
    
    generate_recommendations = PythonOperator(
        task_id='generate_pricing_recommendations',
        python_callable=generate_pricing_recommendations,
        provide_context=True,
    )
    
    record_metrics = PostgresOperator(
        task_id='record_training_metrics',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO metadata.ml_model_metrics (
                model_name, model_version, metric_name, metric_value, trained_at
            ) VALUES 
            ('dynamic_pricing', '1.0', 'r2', {{ task_instance.xcom_pull(task_ids='train_pricing_model', key='metrics')['r2'] }}, CURRENT_TIMESTAMP),
            ('dynamic_pricing', '1.0', 'mae', {{ task_instance.xcom_pull(task_ids='train_pricing_model', key='metrics')['mae'] }}, CURRENT_TIMESTAMP);
        """,
    )
    
    train_task >> generate_recommendations >> record_metrics