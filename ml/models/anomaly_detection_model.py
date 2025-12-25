"""
Anomaly Detection Model
Detects fraudulent bookings, fake reviews, and suspicious user behavior
Uses Isolation Forest and custom rule-based detection
"""
import logging
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
import pickle

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import psycopg2

from ml.features.anomaly_features import AnomalyFeatureEngineer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnomalyDetectionModel:
    """Detect anomalies in bookings, reviews, and user behavior"""
    
    def __init__(self, feature_engineer: AnomalyFeatureEngineer = None):
        """Initialize anomaly detection model"""
        self.feature_engineer = feature_engineer or AnomalyFeatureEngineer()
        self.booking_model = None
        self.review_model = None
        self.scaler = StandardScaler()
        self.feature_names = []
        
        logger.info("✅ Anomaly Detection Model initialized")
    
    def train_booking_anomaly_detector(
        self,
        contamination: float = 0.05,
        random_state: int = 42
    ):
        """
        Train Isolation Forest for booking anomalies
        
        Args:
            contamination: Expected proportion of anomalies
            random_state: Random seed
        """
        logger.info("🚀 Training booking anomaly detector...")
        
        # Extract features
        df = self.feature_engineer.extract_booking_features(lookback_days=180)
        
        # Define features for model
        self.feature_names = [
            'nights',
            'num_guests',
            'price_per_night',
            'price_deviation_from_base',
            'price_deviation_from_avg',
            'lead_time_days',
            'confirmation_time_minutes',
            'booking_hour',
            'is_night_booking',
            'guest_total_bookings',
            'guest_bookings_last_7',
            'guests_exceed_capacity',
            'is_new_user',
            'high_booking_velocity'
        ]
        
        # Prepare feature matrix
        X = df[self.feature_names].fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Train Isolation Forest
        self.booking_model = IsolationForest(
            contamination=contamination,
            random_state=random_state,
            n_estimators=100,
            max_samples='auto',
            n_jobs=-1
        )
        
        self.booking_model.fit(X_scaled)
        
        # Get anomaly scores
        anomaly_scores = self.booking_model.score_samples(X_scaled)
        predictions = self.booking_model.predict(X_scaled)
        
        # Statistics
        n_anomalies = (predictions == -1).sum()
        
        logger.info(f"✅ Booking anomaly detector trained")
        logger.info(f"   Training samples: {len(X)}")
        logger.info(f"   Detected anomalies: {n_anomalies} ({n_anomalies/len(X)*100:.2f}%)")
        logger.info(f"   Anomaly score range: [{anomaly_scores.min():.3f}, {anomaly_scores.max():.3f}]")
    
    def train_review_anomaly_detector(
        self,
        contamination: float = 0.08,
        random_state: int = 42
    ):
        """Train Isolation Forest for review anomalies"""
        logger.info("🚀 Training review anomaly detector...")
        
        # Extract features
        df = self.feature_engineer.extract_review_features(lookback_days=180)
        
        # Define features
        review_features = [
            'rating',
            'sentiment_score',
            'days_after_checkout',
            'review_length',
            'reviewer_total_reviews',
            'reviewer_avg_rating',
            'property_total_reviews',
            'property_avg_rating',
            'review_posted_immediately',
            'review_too_short',
            'rating_sentiment_mismatch',
            'extreme_rating',
            'is_new_reviewer',
            'review_burst'
        ]
        
        # Prepare feature matrix
        X = df[review_features].fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        # Scale
        X_scaled = self.scaler.fit_transform(X)
        
        # Train
        self.review_model = IsolationForest(
            contamination=contamination,
            random_state=random_state,
            n_estimators=100,
            n_jobs=-1
        )
        
        self.review_model.fit(X_scaled)
        
        predictions = self.review_model.predict(X_scaled)
        n_anomalies = (predictions == -1).sum()
        
        logger.info(f"✅ Review anomaly detector trained")
        logger.info(f"   Detected anomalies: {n_anomalies} ({n_anomalies/len(X)*100:.2f}%)")
    
    def detect_booking_anomaly(
        self,
        booking_id: int
    ) -> Dict:
        """
        Detect if a booking is anomalous
        
        Args:
            booking_id: Booking ID to check
            
        Returns:
            Dictionary with anomaly score and flags
        """
        if self.booking_model is None:
            raise ValueError("Booking model not trained")
        
        # Extract features
        df = self.feature_engineer.extract_booking_features(
            booking_ids=[booking_id]
        )
        
        if len(df) == 0:
            raise ValueError(f"Booking {booking_id} not found")
        
        # Prepare features
        X = df[self.feature_names].fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        X_scaled = self.scaler.transform(X)
        
        # Predict
        prediction = self.booking_model.predict(X_scaled)[0]
        anomaly_score = self.booking_model.score_samples(X_scaled)[0]
        
        # Rule-based flags
        flags = self._apply_booking_rules(df.iloc[0])
        
        result = {
            'booking_id': booking_id,
            'is_anomaly': prediction == -1,
            'anomaly_score': float(anomaly_score),
            'risk_level': self._get_risk_level(anomaly_score),
            'flags': flags,
            'recommendation': self._get_recommendation(prediction, flags)
        }
        
        return result
    
    def _apply_booking_rules(self, booking: pd.Series) -> List[str]:
        """Apply rule-based fraud detection"""
        flags = []
        
        # Price anomalies
        if abs(booking['price_deviation_from_base']) > 0.5:
            flags.append('unusual_price')
        
        # Timing anomalies
        if booking['is_night_booking']:
            flags.append('night_booking')
        
        if booking['lead_time_days'] < 1:
            flags.append('last_minute_booking')
        
        # Guest behavior
        if booking['high_booking_velocity']:
            flags.append('high_booking_velocity')
        
        if booking['is_new_user'] and booking['total_price'] > 500:
            flags.append('new_user_high_value')
        
        if booking['guests_exceed_capacity']:
            flags.append('capacity_exceeded')
        
        return flags
    
    def _get_risk_level(self, anomaly_score: float) -> str:
        """Determine risk level from anomaly score"""
        # Lower scores = more anomalous (Isolation Forest)
        if anomaly_score < -0.5:
            return 'high'
        elif anomaly_score < -0.2:
            return 'medium'
        else:
            return 'low'
    
    def _get_recommendation(
        self,
        prediction: int,
        flags: List[str]
    ) -> str:
        """Get recommendation for handling"""
        if prediction == -1 and len(flags) >= 3:
            return 'block'
        elif prediction == -1 or len(flags) >= 2:
            return 'review'
        else:
            return 'approve'
    
    def batch_detect_anomalies(
        self,
        entity_type: str = 'booking',
        lookback_days: int = 7
    ) -> pd.DataFrame:
        """
        Batch anomaly detection
        
        Args:
            entity_type: 'booking' or 'review'
            lookback_days: Days to analyze
            
        Returns:
            DataFrame with anomaly results
        """
        logger.info(f"🔍 Running batch anomaly detection for {entity_type}s...")
        
        if entity_type == 'booking':
            if self.booking_model is None:
                raise ValueError("Booking model not trained")
            
            # Extract recent bookings
            df = self.feature_engineer.extract_booking_features(
                lookback_days=lookback_days
            )
            
            # Prepare features
            X = df[self.feature_names].fillna(0)
            X = X.replace([np.inf, -np.inf], 0)
            X_scaled = self.scaler.transform(X)
            
            # Predict
            predictions = self.booking_model.predict(X_scaled)
            anomaly_scores = self.booking_model.score_samples(X_scaled)
            
            # Add to dataframe
            df['is_anomaly'] = predictions == -1
            df['anomaly_score'] = anomaly_scores
            df['risk_level'] = df['anomaly_score'].apply(self._get_risk_level)
            
            # Apply rules
            df['flags'] = df.apply(
                lambda row: ','.join(self._apply_booking_rules(row)),
                axis=1
            )
            
            results = df[[
                'booking_id', 'property_id', 'guest_id',
                'is_anomaly', 'anomaly_score', 'risk_level', 'flags'
            ]]
            
        else:  # review
            if self.review_model is None:
                raise ValueError("Review model not trained")
            
            # Similar implementation for reviews
            results = pd.DataFrame()
        
        logger.info(f"✅ Detected {results['is_anomaly'].sum()} anomalies")
        
        return results
    
    def save_to_database(self, results: pd.DataFrame):
        """Save anomaly detection results to database"""
        conn = psycopg2.connect(**self.feature_engineer.db_config)
        cursor = conn.cursor()
        
        for _, row in results.iterrows():
            if row['is_anomaly']:
                cursor.execute("""
                    INSERT INTO metadata.anomaly_detections (
                        entity_type, entity_id, anomaly_score, risk_level, flags, detected_at
                    ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (entity_type, entity_id, detected_at::date) DO UPDATE SET
                        anomaly_score = EXCLUDED.anomaly_score,
                        risk_level = EXCLUDED.risk_level,
                        flags = EXCLUDED.flags
                """, (
                    'booking',
                    int(row['booking_id']),
                    float(row['anomaly_score']),
                    row['risk_level'],
                    row['flags']
                ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Saved {len(results)} anomaly records to database")
    
    def save_model(self, filepath: str):
        """Save model to disk"""
        model_data = {
            'booking_model': self.booking_model,
            'review_model': self.review_model,
            'scaler': self.scaler,
            'feature_names': self.feature_names
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        logger.info(f"✅ Model saved to {filepath}")
    
    def load_model(self, filepath: str):
        """Load model from disk"""
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.booking_model = model_data['booking_model']
        self.review_model = model_data['review_model']
        self.scaler = model_data['scaler']
        self.feature_names = model_data['feature_names']
        
        logger.info(f"✅ Model loaded from {filepath}")


# Example usage
if __name__ == "__main__":
    model = AnomalyDetectionModel()
    
    # Train models
    model.train_booking_anomaly_detector(contamination=0.05)
    model.train_review_anomaly_detector(contamination=0.08)
    
    # Detect anomaly for specific booking
    result = model.detect_booking_anomaly(booking_id=12345)
    print(f"\n🔍 Anomaly Detection Result: {result}")
    
    # Batch detection
    anomalies = model.batch_detect_anomalies(entity_type='booking', lookback_days=7)
    print(f"\n📊 Recent Anomalies:\n{anomalies[anomalies['is_anomaly']].head()}")
    
    # Save model
    model.save_model('/tmp/anomaly_detection_model.pkl')