"""
Feature Engineering for Dynamic Pricing Model
Extracts and transforms features for price prediction
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PricingFeatureEngineer:
    """Feature engineering for dynamic pricing"""
    
    def __init__(self, db_config: Dict = None):
        """Initialize feature engineer"""
        self.db_config = db_config or {
            'host': 'postgres',
            'database': 'airstay_db',
            'user': 'airstay',
            'password': 'airstay_pass'
        }
        
        # Label encoders (fit during training)
        self.encoders = {}
        
        logger.info("✅ Pricing Feature Engineer initialized")
    
    def _get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)
    
    def extract_property_features(self, property_ids: List[int] = None) -> pd.DataFrame:
        """
        Extract property-level features
        
        Args:
            property_ids: Optional list of property IDs to filter
            
        Returns:
            DataFrame with property features
        """
        conn = self._get_db_connection()
        
        query = """
            SELECT 
                p.property_id,
                p.listing_id,
                p.property_type,
                p.bedrooms,
                p.bathrooms,
                p.beds,
                p.max_guests,
                p.location_city,
                p.location_state,
                p.neighborhood,
                p.latitude,
                p.longitude,
                COALESCE(array_length(p.amenities, 1), 0) as amenities_count,
                p.instant_bookable,
                p.minimum_nights,
                p.cancellation_policy,
                p.host_is_superhost,
                COALESCE(p.property_rating, 0) as property_rating,
                COALESCE(p.review_count, 0) as review_count,
                COALESCE(p.quality_score, 0) as quality_score,
                p.base_price,
                -- Host features
                h.host_since,
                COALESCE(h.host_response_rate, 0) as host_response_rate,
                COALESCE(h.host_acceptance_rate, 0) as host_acceptance_rate,
                COALESCE(h.total_listings, 1) as host_total_listings
            FROM silver.properties p
            LEFT JOIN silver.hosts h ON p.host_id = h.host_id
            WHERE p.is_active = TRUE
        """
        
        params = []
        if property_ids:
            query += " AND p.property_id = ANY(%s)"
            params.append(property_ids)
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        logger.info(f"Extracted {len(df)} property records")
        
        return df
    
    def extract_booking_history(
        self,
        property_ids: List[int] = None,
        lookback_days: int = 180
    ) -> pd.DataFrame:
        """
        Extract historical booking data for properties
        
        Args:
            property_ids: Optional list of property IDs
            lookback_days: Days of history to include
            
        Returns:
            DataFrame with booking history
        """
        conn = self._get_db_connection()
        
        query = """
            SELECT 
                property_id,
                COUNT(*) as total_bookings,
                COUNT(*) FILTER (WHERE booking_status = 'completed') as completed_bookings,
                COUNT(*) FILTER (WHERE booking_status = 'canceled') as canceled_bookings,
                AVG(nights) as avg_nights_booked,
                AVG(num_guests) as avg_guests,
                AVG(total_price) as avg_booking_price,
                STDDEV(total_price) as stddev_price,
                MIN(total_price) as min_price,
                MAX(total_price) as max_price,
                -- Recent bookings (last 30 days)
                COUNT(*) FILTER (
                    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
                ) as bookings_last_30_days,
                AVG(total_price) FILTER (
                    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
                ) as avg_price_last_30_days
            FROM silver.bookings
            WHERE created_at >= CURRENT_DATE - INTERVAL '%s days'
        """
        
        params = [lookback_days]
        
        if property_ids:
            query += " AND property_id = ANY(%s)"
            params.append(property_ids)
        
        query += " GROUP BY property_id"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # Fill NaN values
        df = df.fillna(0)
        
        logger.info(f"Extracted booking history for {len(df)} properties")
        
        return df
    
    def extract_market_features(self) -> pd.DataFrame:
        """
        Extract market-level features (city, neighborhood)
        
        Returns:
            DataFrame with market features
        """
        conn = self._get_db_connection()
        
        query = """
            SELECT 
                location_city as city,
                COUNT(*) as properties_in_city,
                AVG(base_price) as city_avg_price,
                STDDEV(base_price) as city_price_stddev,
                AVG(property_rating) as city_avg_rating,
                AVG(review_count) as city_avg_reviews,
                -- Occupancy proxy
                COUNT(*) FILTER (WHERE instant_bookable = TRUE) as instant_bookable_count
            FROM silver.properties
            WHERE is_active = TRUE
            GROUP BY location_city
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        df = df.fillna(0)
        
        logger.info(f"Extracted market features for {len(df)} cities")
        
        return df
    
    def calculate_temporal_features(self, target_date: datetime = None) -> Dict:
        """
        Calculate temporal features for a given date
        
        Args:
            target_date: Date to calculate features for (default: today)
            
        Returns:
            Dictionary of temporal features
        """
        if target_date is None:
            target_date = datetime.now()
        
        features = {
            'day_of_week': target_date.weekday(),  # 0=Monday, 6=Sunday
            'is_weekend': 1 if target_date.weekday() >= 5 else 0,
            'month': target_date.month,
            'quarter': (target_date.month - 1) // 3 + 1,
            'day_of_month': target_date.day,
            'week_of_year': target_date.isocalendar()[1],
            'is_holiday': self._is_holiday(target_date),
            'season': self._get_season(target_date.month),
        }
        
        return features
    
    def _is_holiday(self, date: datetime) -> int:
        """Check if date is a US holiday"""
        # Major US holidays (simplified)
        holidays = [
            (1, 1),   # New Year's Day
            (7, 4),   # Independence Day
            (12, 25), # Christmas
            (11, 25), # Thanksgiving (approximate)
        ]
        
        return 1 if (date.month, date.day) in holidays else 0
    
    def _get_season(self, month: int) -> str:
        """Get season from month"""
        if month in [12, 1, 2]:
            return 'winter'
        elif month in [3, 4, 5]:
            return 'spring'
        elif month in [6, 7, 8]:
            return 'summer'
        else:
            return 'fall'
    
    def calculate_competition_features(
        self,
        property_id: int,
        radius_km: float = 3.0
    ) -> Dict:
        """
        Calculate competitive features (nearby properties)
        
        Args:
            property_id: Property ID
            radius_km: Search radius in km
            
        Returns:
            Dictionary of competition features
        """
        conn = self._get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get property location
        cursor.execute(
            "SELECT latitude, longitude FROM silver.properties WHERE property_id = %s",
            (property_id,)
        )
        prop = cursor.fetchone()
        
        if not prop:
            return {}
        
        # Find nearby properties
        query = """
            SELECT 
                COUNT(*) as nearby_properties,
                AVG(base_price) as nearby_avg_price,
                MIN(base_price) as nearby_min_price,
                MAX(base_price) as nearby_max_price,
                AVG(property_rating) as nearby_avg_rating
            FROM silver.properties
            WHERE 
                is_active = TRUE
                AND property_id != %s
                AND ST_DWithin(
                    location_point,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                    %s * 1000
                )
        """
        
        cursor.execute(
            query,
            (property_id, prop['longitude'], prop['latitude'], radius_km)
        )
        
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return dict(result) if result else {}
    
    def build_feature_matrix(
        self,
        property_ids: List[int] = None,
        target_date: datetime = None,
        include_target: bool = True
    ) -> pd.DataFrame:
        """
        Build complete feature matrix for training/prediction
        
        Args:
            property_ids: Optional list of property IDs
            target_date: Date for temporal features
            include_target: Whether to include target variable (base_price)
            
        Returns:
            DataFrame with all features
        """
        logger.info("🔧 Building feature matrix...")
        
        # Extract property features
        properties_df = self.extract_property_features(property_ids)
        
        # Extract booking history
        bookings_df = self.extract_booking_history(property_ids)
        
        # Extract market features
        market_df = self.extract_market_features()
        
        # Merge dataframes
        df = properties_df.merge(
            bookings_df,
            on='property_id',
            how='left'
        ).merge(
            market_df,
            left_on='location_city',
            right_on='city',
            how='left'
        )
        
        # Fill NaN values from merge
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
        
        # Add temporal features
        temporal_features = self.calculate_temporal_features(target_date)
        for key, value in temporal_features.items():
            df[key] = value
        
        # Encode categorical variables
        categorical_columns = [
            'property_type', 'location_city', 'cancellation_policy', 'season'
        ]
        
        for col in categorical_columns:
            if col in df.columns:
                if col not in self.encoders:
                    self.encoders[col] = LabelEncoder()
                    df[f'{col}_encoded'] = self.encoders[col].fit_transform(df[col].astype(str))
                else:
                    # Handle unseen categories
                    df[f'{col}_encoded'] = df[col].apply(
                        lambda x: self.encoders[col].transform([str(x)])[0]
                        if str(x) in self.encoders[col].classes_
                        else -1
                    )
        
        # Derived features
        df['price_per_guest'] = df['base_price'] / df['max_guests'].replace(0, 1)
        df['price_per_bedroom'] = df['base_price'] / df['bedrooms'].replace(0, 1)
        df['occupancy_rate'] = (
            df['bookings_last_30_days'] / 30 * 100
        ).clip(0, 100)
        df['price_competitiveness'] = (
            df['base_price'] / df['city_avg_price'].replace(0, 1)
        )
        df['host_experience_days'] = (
            (datetime.now() - pd.to_datetime(df['host_since'])).dt.days
        ).fillna(0)
        
        # Boolean conversions
        df['instant_bookable'] = df['instant_bookable'].astype(int)
        df['host_is_superhost'] = df['host_is_superhost'].astype(int)
        
        logger.info(f"✅ Built feature matrix: {df.shape}")
        
        if not include_target:
            df = df.drop(columns=['base_price'], errors='ignore')
        
        return df
    
    def get_feature_names(self) -> List[str]:
        """Get list of feature names for model training"""
        features = [
            # Property features
            'bedrooms', 'bathrooms', 'beds', 'max_guests',
            'amenities_count', 'instant_bookable', 'minimum_nights',
            'property_rating', 'review_count', 'quality_score',
            
            # Host features
            'host_is_superhost', 'host_response_rate', 'host_acceptance_rate',
            'host_total_listings', 'host_experience_days',
            
            # Booking history
            'total_bookings', 'completed_bookings', 'canceled_bookings',
            'avg_nights_booked', 'avg_guests', 'bookings_last_30_days',
            'occupancy_rate',
            
            # Market features
            'properties_in_city', 'city_avg_price', 'city_avg_rating',
            'city_avg_reviews', 'price_competitiveness',
            
            # Temporal features
            'day_of_week', 'is_weekend', 'month', 'quarter',
            'week_of_year', 'is_holiday',
            
            # Derived features
            'price_per_guest', 'price_per_bedroom',
            
            # Encoded categoricals
            'property_type_encoded', 'location_city_encoded',
            'cancellation_policy_encoded', 'season_encoded',
        ]
        
        return features


# Example usage
if __name__ == "__main__":
    engineer = PricingFeatureEngineer()
    
    # Build feature matrix
    df = engineer.build_feature_matrix(include_target=True)
    
    print(f"\n📊 Feature Matrix Shape: {df.shape}")
    print(f"\n🔢 Sample Features:\n{df.head()}")
    print(f"\n📋 Feature Names: {engineer.get_feature_names()}")