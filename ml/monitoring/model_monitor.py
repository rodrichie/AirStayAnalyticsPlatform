"""
ML Model Monitoring System
Tracks model performance, data drift, and prediction quality over time
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import json
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ModelMonitor:
    """Monitor ML model performance and data quality"""

    def __init__(self, db_config: Dict = None):
        """Initialize monitor with database configuration"""
        self.db_config = db_config or {
            'host': 'postgres',
            'database': 'airstay_db',
            'user': 'airstay',
            'password': 'airstay_pass'
        }

        logger.info("✅ Model Monitor initialized")

    def _get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)    
    
    def log_prediction(
        self,
        model_name: str,
        model_version: str,
        entity_id: int,
        prediction: float,
        features: Dict,
        actual_value: Optional[float] = None,
        metadata: Dict = None
    ):
        """
        Log a model prediction for monitoring
        
        Args:
            model_name: Name of model
            model_version: Model version
            entity_id: Entity being predicted (property_id, booking_id, etc.)
            prediction: Predicted value
            features: Input features used
            actual_value: Actual outcome (when available)
            metadata: Additional metadata
        """
        conn = self._get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO metadata.model_predictions (
                model_name, model_version, entity_id, prediction, 
                features, actual_value, metadata, predicted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, (
            model_name,
            model_version,
            entity_id,
            prediction,
            json.dumps(features),
            actual_value,
            json.dumps(metadata or {})
        ))
        
        conn.commit()
        cursor.close()
        conn.close()

    def calculate_prediction_metrics(
        self,
        model_name: str,
        lookback_days: int = 7
    ) -> Dict:
        """
        Calculate prediction accuracy metrics
        
        Args:
            model_name: Model to evaluate
            lookback_days: Days of predictions to analyze
            
        Returns:
            Dictionary of metrics
        """
        conn = self._get_db_connection()

        query = """
            SELECT 
                prediction,
                actual_value,
                predicted_at
            FROM metadata.model_predictions
            WHERE 
                model_name = %s
                AND actual_value IS NOT NULL
                AND predicted_at >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY predicted_at
        """
        
        df = pd.read_sql_query(query, conn, params=[model_name, lookback_days])
        conn.close()

        if len(df) == 0:
            logger.warning(f"No predictions with actuals for {model_name}")
            return {}
        
        # Calculate metrics
        predictions = df['prediction'].values
        actuals = df['actual_value'].values
        
        mae = np.mean(np.abs(predictions - actuals))
        rmse = np.sqrt(np.mean((predictions - actuals) ** 2))
        mape = np.mean(np.abs((actuals - predictions) / actuals)) * 100
        
        # Bias (systematic over/under prediction)
        bias = np.mean(predictions - actuals)

        metrics = {
            'model_name': model_name,
            'evaluation_period_days': lookback_days,
            'n_predictions': len(df),
            'mae': round(float(mae), 4),
            'rmse': round(float(rmse), 4),
            'mape': round(float(mape), 4),
            'bias': round(float(bias), 4),
            'evaluated_at': datetime.now().isoformat()
        }
        
        logger.info(f"📊 {model_name} Metrics: MAE={metrics['mae']}, RMSE={metrics['rmse']}")
        
        return metrics

    def detect_data_drift(
        self,
        model_name: str,
        feature_name: str,
        baseline_days: int = 30,
        current_days: int = 7
    ) -> Dict:
        """
        Detect data drift by comparing feature distributions
        
        Args:
            model_name: Model to check
            feature_name: Feature to analyze
            baseline_days: Days for baseline distribution
            current_days: Days for current distribution
            
        Returns:
            Dictionary with drift metrics
        """
        conn = self._get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get baseline distribution
        cursor.execute("""
            SELECT 
                (features->>%s)::float as feature_value
            FROM metadata.model_predictions
            WHERE 
                model_name = %s
                AND predicted_at >= CURRENT_DATE - INTERVAL '%s days'
                AND predicted_at < CURRENT_DATE - INTERVAL '%s days'
                AND features ? %s
        """, (
            feature_name, model_name,
            baseline_days + current_days, current_days,
            feature_name
        ))

        baseline = [row['feature_value'] for row in cursor.fetchall()]
        
        # Get current distribution
        cursor.execute("""
            SELECT 
                (features->>%s)::float as feature_value
            FROM metadata.model_predictions
            WHERE 
                model_name = %s
                AND predicted_at >= CURRENT_DATE - INTERVAL '%s days'
                AND features ? %s
        """, (feature_name, model_name, current_days, feature_name))
        
        current = [row['feature_value'] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()

        if len(baseline) == 0 or len(current) == 0:
            logger.warning("Insufficient data for drift detection")
            return {}
        
        # Calculate distribution statistics
        baseline_mean = np.mean(baseline)
        baseline_std = np.std(baseline)
        current_mean = np.mean(current)
        current_std = np.std(current)

        # Population Stability Index (PSI)
        # Simplified version - divide into deciles
        baseline_arr = np.array(baseline)
        current_arr = np.array(current)
        
        bins = np.percentile(baseline_arr, np.arange(0, 101, 10))
        baseline_dist = np.histogram(baseline_arr, bins=bins)[0] / len(baseline_arr)
        current_dist = np.histogram(current_arr, bins=bins)[0] / len(current_arr)
        
        # Avoid division by zero
        baseline_dist = np.where(baseline_dist == 0, 0.0001, baseline_dist)
        current_dist = np.where(current_dist == 0, 0.0001, current_dist)
        
        psi = np.sum((current_dist - baseline_dist) * np.log(current_dist / baseline_dist))
        
        # Interpret PSI
        if psi < 0.1:
            drift_level = 'low'
        elif psi < 0.2:
            drift_level = 'medium'
        else:
            drift_level = 'high'

        drift_metrics = {
            'model_name': model_name,
            'feature_name': feature_name,
            'baseline_mean': round(float(baseline_mean), 4),
            'baseline_std': round(float(baseline_std), 4),
            'current_mean': round(float(current_mean), 4),
            'current_std': round(float(current_std), 4),
            'mean_shift': round(float(current_mean - baseline_mean), 4),
            'psi': round(float(psi), 4),
            'drift_level': drift_level,
            'checked_at': datetime.now().isoformat()
        }
        
        logger.info(f"📈 Drift Detection: {feature_name} PSI={drift_metrics['psi']:.4f} ({drift_level})")
        
        return drift_metrics

    def monitor_prediction_latency(
        self,
        model_name: str,
        lookback_hours: int = 24
    ) -> Dict:
        """
        Monitor prediction latency (time from request to response)
        
        Args:
            model_name: Model to monitor
            lookback_hours: Hours to analyze
            
        Returns:
            Latency statistics
        """
        conn = self._get_db_connection()
        
        query = """
            SELECT 
                EXTRACT(EPOCH FROM (predicted_at - (metadata->>'request_time')::timestamp)) * 1000 as latency_ms
            FROM metadata.model_predictions
            WHERE 
                model_name = %s
                AND predicted_at >= CURRENT_TIMESTAMP - INTERVAL '%s hours'
                AND metadata ? 'request_time'
        """  
        df = pd.read_sql_query(query, conn, params=[model_name, lookback_hours])
        conn.close()
        
        if len(df) == 0:
            return {}
        
        latencies = df['latency_ms'].values
        
        return {
            'model_name': model_name,
            'p50_latency_ms': round(float(np.percentile(latencies, 50)), 2),
            'p95_latency_ms': round(float(np.percentile(latencies, 95)), 2),
            'p99_latency_ms': round(float(np.percentile(latencies, 99)), 2),
            'max_latency_ms': round(float(np.max(latencies)), 2),
            'avg_latency_ms': round(float(np.mean(latencies)), 2)
        }


    def generate_monitoring_report(
        self,
        model_name: str
    ) -> Dict:
        """
        Generate comprehensive monitoring report
        
        Args:
            model_name: Model to report on
            
        Returns:
            Complete monitoring report
        """
        logger.info(f"📊 Generating monitoring report for {model_name}...")

        report = {
            'model_name': model_name,
            'report_generated_at': datetime.now().isoformat(),
            'prediction_metrics': self.calculate_prediction_metrics(model_name, lookback_days=7),
            'latency_metrics': self.monitor_prediction_latency(model_name, lookback_hours=24),
            'drift_alerts': []
        } 

        # Check drift for common features
        common_features = ['base_price', 'bedrooms', 'nights', 'lead_time_days']
        
        for feature in common_features:
            try:
                drift = self.detect_data_drift(model_name, feature)
                if drift.get('drift_level') in ['medium', 'high']:
                    report['drift_alerts'].append(drift)
            except Exception as e:
                logger.warning(f"Could not check drift for {feature}: {e}")
        
        return report
    
     

