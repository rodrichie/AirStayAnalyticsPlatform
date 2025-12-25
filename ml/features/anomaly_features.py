"""
Feature Engineering for Anomaly Detection
Extracts features to detect fraudulent bookings, fake reviews, and suspicious behavior
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnomalyFeatureEngineer:
    """Extract features for anomaly detection"""
    
    def __init__(self, db_config: Dict = None):
        """Initialize feature engineer"""
        self.db_config = db_config or {
            'host': 'postgres',
            'database': 'airstay_db',
            'user': 'airstay',
            'password': 'airstay_pass'
        }
        
        logger.info("✅ Anomaly Feature Engineer initialized")
    
    def _get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)
    
    def extract_booking_features(
        self,
        booking_ids: List[int] = None,
        lookback_days: int = 90
    ) -> pd.DataFrame:
        """
        Extract booking-level features for fraud detection
        
        Features include:
        - Booking velocity (bookings per user/property)
        - Price anomalies
        - Time-based patterns
        - Guest behavior patterns
        
        Args:
            booking_ids: Specific bookings to analyze
            lookback_days: Days of history for context
            
        Returns:
            DataFrame with anomaly features
        """
        conn = self._get_db_connection()
        
        query = """
            WITH booking_data AS (
                SELECT 
                    b.booking_id,
                    b.property_id,
                    b.guest_id,
                    b.check_in_date,
                    b.check_out_date,
                    b.nights,
                    b.num_guests,
                    b.total_price,
                    b.booking_status,
                    b.booking_channel,
                    b.created_at,
                    b.confirmed_at,
                    p.base_price,
                    p.location_city,
                    p.property_type,
                    p.max_guests,
                    -- Guest history
                    COUNT(*) OVER (PARTITION BY b.guest_id) as guest_total_bookings,
                    COUNT(*) OVER (
                        PARTITION BY b.guest_id 
                        ORDER BY b.created_at 
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) as guest_bookings_last_7,
                    -- Property history
                    COUNT(*) OVER (PARTITION BY b.property_id) as property_total_bookings,
                    AVG(b.total_price) OVER (PARTITION BY b.property_id) as property_avg_price,
                    STDDEV(b.total_price) OVER (PARTITION BY b.property_id) as property_price_stddev
                FROM silver.bookings b
                JOIN silver.properties p ON b.property_id = p.property_id
                WHERE b.created_at >= CURRENT_DATE - INTERVAL '%s days'
            )
            SELECT 
                booking_id,
                property_id,
                guest_id,
                check_in_date,
                check_out_date,
                nights,
                num_guests,
                total_price,
                booking_status,
                booking_channel,
                created_at,
                confirmed_at,
                base_price,
                location_city,
                property_type,
                max_guests,
                guest_total_bookings,
                guest_bookings_last_7,
                property_total_bookings,
                property_avg_price,
                COALESCE(property_price_stddev, 0) as property_price_stddev
            FROM booking_data
        """
        
        params = [lookback_days]
        
        if booking_ids:
            query += " WHERE booking_id = ANY(%s)"
            params.append(booking_ids)
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # Calculate derived features
        df['price_per_night'] = df['total_price'] / df['nights'].replace(0, 1)
        df['price_deviation_from_base'] = (df['price_per_night'] - df['base_price']) / df['base_price'].replace(0, 1)
        df['price_deviation_from_avg'] = (
            (df['total_price'] - df['property_avg_price']) / 
            df['property_price_stddev'].replace(0, 1)
        )
        
        # Time-based features
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['check_in_date'] = pd.to_datetime(df['check_in_date'])
        df['confirmed_at'] = pd.to_datetime(df['confirmed_at'])
        
        df['lead_time_days'] = (df['check_in_date'] - df['created_at']).dt.days
        df['confirmation_time_minutes'] = (
            (df['confirmed_at'] - df['created_at']).dt.total_seconds() / 60
        ).fillna(0)
        
        df['booking_hour'] = df['created_at'].dt.hour
        df['booking_day_of_week'] = df['created_at'].dt.dayofweek
        df['is_night_booking'] = ((df['booking_hour'] >= 22) | (df['booking_hour'] <= 5)).astype(int)
        
        # Guest capacity anomaly
        df['guests_exceed_capacity'] = (df['num_guests'] > df['max_guests']).astype(int)
        
        # New user flag
        df['is_new_user'] = (df['guest_total_bookings'] == 1).astype(int)
        
        # Velocity flags
        df['high_booking_velocity'] = (df['guest_bookings_last_7'] > 3).astype(int)
        
        logger.info(f"Extracted features for {len(df)} bookings")
        
        return df
    
    def extract_review_features(
        self,
        review_ids: List[int] = None,
        lookback_days: int = 90
    ) -> pd.DataFrame:
        """
        Extract review-level features for fake review detection
        
        Features include:
        - Review patterns (length, sentiment, timing)
        - Reviewer behavior
        - Review-booking relationship
        
        Args:
            review_ids: Specific reviews to analyze
            lookback_days: Days of history
            
        Returns:
            DataFrame with review anomaly features
        """
        conn = self._get_db_connection()
        
        query = """
            WITH review_data AS (
                SELECT 
                    r.review_id,
                    r.property_id,
                    r.booking_id,
                    r.guest_id,
                    r.rating,
                    r.review_text,
                    r.sentiment_score,
                    r.sentiment_label,
                    r.created_at,
                    b.check_out_date,
                    b.nights as booking_nights,
                    -- Reviewer history
                    COUNT(*) OVER (PARTITION BY r.guest_id) as reviewer_total_reviews,
                    AVG(r.rating) OVER (PARTITION BY r.guest_id) as reviewer_avg_rating,
                    STDDEV(r.rating) OVER (PARTITION BY r.guest_id) as reviewer_rating_stddev,
                    -- Property review stats
                    COUNT(*) OVER (PARTITION BY r.property_id) as property_total_reviews,
                    AVG(r.rating) OVER (PARTITION BY r.property_id) as property_avg_rating,
                    -- Recent review patterns
                    COUNT(*) OVER (
                        PARTITION BY r.property_id 
                        ORDER BY r.created_at 
                        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                    ) as property_recent_reviews
                FROM silver.reviews r
                LEFT JOIN silver.bookings b ON r.booking_id = b.booking_id
                WHERE r.created_at >= CURRENT_DATE - INTERVAL '%s days'
            )
            SELECT *
            FROM review_data
        """
        
        params = [lookback_days]
        
        if review_ids:
            query += " WHERE review_id = ANY(%s)"
            params.append(review_ids)
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # Calculate derived features
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['check_out_date'] = pd.to_datetime(df['check_out_date'])
        
        # Review timing
        df['days_after_checkout'] = (df['created_at'] - df['check_out_date']).dt.days
        df['review_posted_immediately'] = (df['days_after_checkout'] <= 1).astype(int)
        df['review_posted_very_late'] = (df['days_after_checkout'] > 90).astype(int)
        
        # Review content features
        df['review_length'] = df['review_text'].fillna('').str.len()
        df['review_too_short'] = (df['review_length'] < 20).astype(int)
        df['review_too_long'] = (df['review_length'] > 2000).astype(int)
        
        # Rating anomalies
        df['rating_sentiment_mismatch'] = (
            ((df['rating'] >= 4) & (df['sentiment_score'] < 0)) |
            ((df['rating'] <= 2) & (df['sentiment_score'] > 0))
        ).astype(int)
        
        df['extreme_rating'] = ((df['rating'] == 1) | (df['rating'] == 5)).astype(int)
        
        # Reviewer patterns
        df['is_new_reviewer'] = (df['reviewer_total_reviews'] == 1).astype(int)
        df['reviewer_always_extreme'] = (
            (df['reviewer_rating_stddev'].fillna(0) < 0.5) & 
            (df['extreme_rating'] == 1)
        ).astype(int)
        
        # Property review burst
        df['review_burst'] = (df['property_recent_reviews'] > 5).astype(int)
        
        logger.info(f"Extracted review features for {len(df)} reviews")
        
        return df
    
    def extract_user_behavior_features(
        self,
        user_ids: List[int] = None,
        lookback_days: int = 90
    ) -> pd.DataFrame:
        """
        Extract user-level behavior features
        
        Features include:
        - Account age and activity
        - Booking patterns
        - Cancellation rates
        - Review patterns
        
        Args:
            user_ids: Specific users to analyze
            lookback_days: Days of history
            
        Returns:
            DataFrame with user behavior features
        """
        conn = self._get_db_connection()
        
        query = """
            WITH user_bookings AS (
                SELECT 
                    guest_id as user_id,
                    COUNT(*) as total_bookings,
                    COUNT(*) FILTER (WHERE booking_status = 'completed') as completed_bookings,
                    COUNT(*) FILTER (WHERE booking_status = 'canceled') as canceled_bookings,
                    AVG(total_price) as avg_booking_value,
                    STDDEV(total_price) as booking_value_stddev,
                    MIN(created_at) as first_booking_date,
                    MAX(created_at) as last_booking_date,
                    -- Booking velocity
                    COUNT(*) FILTER (
                        WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                    ) as bookings_last_7_days,
                    COUNT(*) FILTER (
                        WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
                    ) as bookings_last_30_days
                FROM silver.bookings
                WHERE created_at >= CURRENT_DATE - INTERVAL '%s days'
                GROUP BY guest_id
            ),
            user_reviews AS (
                SELECT 
                    guest_id as user_id,
                    COUNT(*) as total_reviews,
                    AVG(rating) as avg_rating_given,
                    STDDEV(rating) as rating_stddev
                FROM silver.reviews
                WHERE created_at >= CURRENT_DATE - INTERVAL '%s days'
                GROUP BY guest_id
            )
            SELECT 
                ub.user_id,
                COALESCE(ub.total_bookings, 0) as total_bookings,
                COALESCE(ub.completed_bookings, 0) as completed_bookings,
                COALESCE(ub.canceled_bookings, 0) as canceled_bookings,
                COALESCE(ub.avg_booking_value, 0) as avg_booking_value,
                COALESCE(ub.booking_value_stddev, 0) as booking_value_stddev,
                ub.first_booking_date,
                ub.last_booking_date,
                COALESCE(ub.bookings_last_7_days, 0) as bookings_last_7_days,
                COALESCE(ub.bookings_last_30_days, 0) as bookings_last_30_days,
                COALESCE(ur.total_reviews, 0) as total_reviews,
                COALESCE(ur.avg_rating_given, 0) as avg_rating_given,
                COALESCE(ur.rating_stddev, 0) as rating_stddev
            FROM user_bookings ub
            LEFT JOIN user_reviews ur ON ub.user_id = ur.user_id
        """
        
        params = [lookback_days, lookback_days]
        
        if user_ids:
            query += " WHERE ub.user_id = ANY(%s)"
            params.append(user_ids)
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # Calculate derived features
        df['first_booking_date'] = pd.to_datetime(df['first_booking_date'])
        df['last_booking_date'] = pd.to_datetime(df['last_booking_date'])
        
        df['account_age_days'] = (datetime.now() - df['first_booking_date']).dt.days
        df['days_since_last_booking'] = (datetime.now() - df['last_booking_date']).dt.days
        
        df['cancellation_rate'] = df['canceled_bookings'] / df['total_bookings'].replace(0, 1)
        df['review_rate'] = df['total_reviews'] / df['completed_bookings'].replace(0, 1)
        
        # Anomaly flags
        df['high_cancellation_rate'] = (df['cancellation_rate'] > 0.5).astype(int)
        df['very_new_account'] = (df['account_age_days'] < 7).astype(int)
        df['high_booking_frequency'] = (df['bookings_last_7_days'] > 3).astype(int)
        df['never_reviews'] = ((df['completed_bookings'] > 2) & (df['total_reviews'] == 0)).astype(int)
        
        logger.info(f"Extracted user behavior features for {len(df)} users")
        
        return df


# Example usage
if __name__ == "__main__":
    engineer = AnomalyFeatureEngineer()
    
    # Extract booking features
    booking_features = engineer.extract_booking_features(lookback_days=90)
    print(f"\n📊 Booking Features: {booking_features.shape}")
    print(booking_features.head())
    
    # Extract review features
    review_features = engineer.extract_review_features(lookback_days=90)
    print(f"\n📊 Review Features: {review_features.shape}")