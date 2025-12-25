"""
Feature Engineering for Recommendation System
Prepares user-property interaction data for collaborative filtering
"""
import logging
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationDataProcessor:
    """Process data for recommendation engine"""
    
    def __init__(self, db_config: Dict = None):
        """Initialize processor"""
        self.db_config = db_config or {
            'host': 'postgres',
            'database': 'airstay_db',
            'user': 'airstay',
            'password': 'airstay_pass'
        }
        
        self.user_id_map = {}
        self.property_id_map = {}
        self.reverse_user_map = {}
        self.reverse_property_map = {}
        
        logger.info("✅ Recommendation Data Processor initialized")
    
    def _get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)
    
    def extract_interaction_data(
        self,
        min_interactions: int = 2,
        lookback_days: int = 365
    ) -> pd.DataFrame:
        """
        Extract user-property interactions from multiple sources
        
        Interactions include:
        - Bookings (weight: 5.0)
        - Reviews (weight: 4.0)
        - Property views (weight: 1.0)
        - Searches in city (weight: 0.5)
        
        Args:
            min_interactions: Minimum interactions per user
            lookback_days: Days of history to include
            
        Returns:
            DataFrame with user_id, property_id, interaction_weight
        """
        conn = self._get_db_connection()
        
        # 1. Booking interactions (highest weight)
        booking_query = """
            SELECT 
                guest_id as user_id,
                property_id,
                5.0 as weight,
                'booking' as interaction_type,
                created_at as interaction_time
            FROM silver.bookings
            WHERE 
                created_at >= CURRENT_DATE - INTERVAL '%s days'
                AND booking_status IN ('confirmed', 'completed')
        """
        
        bookings = pd.read_sql_query(
            booking_query, 
            conn, 
            params=[lookback_days]
        )
        
        # 2. Review interactions
        review_query = """
            SELECT 
                guest_id as user_id,
                property_id,
                CASE 
                    WHEN rating >= 4 THEN 4.0
                    WHEN rating >= 3 THEN 2.0
                    ELSE 1.0
                END as weight,
                'review' as interaction_type,
                created_at as interaction_time
            FROM silver.reviews
            WHERE created_at >= CURRENT_DATE - INTERVAL '%s days'
        """
        
        reviews = pd.read_sql_query(
            review_query,
            conn,
            params=[lookback_days]
        )
        
        # 3. Property view interactions (if we track this)
        # For now, we'll simulate based on search patterns
        search_query = """
            SELECT 
                user_id,
                0.5 as weight,
                'search' as interaction_type,
                window_start as interaction_time
            FROM silver.search_trends_cities
            WHERE window_start >= CURRENT_DATE - INTERVAL '%s days'
        """
        
        # Combine all interactions
        interactions = pd.concat([bookings, reviews], ignore_index=True)
        
        conn.close()
        
        # Aggregate by user-property pair
        aggregated = interactions.groupby(['user_id', 'property_id']).agg({
            'weight': 'sum',
            'interaction_time': 'max'
        }).reset_index()
        
        # Filter users with minimum interactions
        user_counts = aggregated['user_id'].value_counts()
        valid_users = user_counts[user_counts >= min_interactions].index
        
        filtered = aggregated[aggregated['user_id'].isin(valid_users)]
        
        logger.info(f"📊 Interaction Stats:")
        logger.info(f"   Total interactions: {len(filtered)}")
        logger.info(f"   Unique users: {filtered['user_id'].nunique()}")
        logger.info(f"   Unique properties: {filtered['property_id'].nunique()}")
        logger.info(f"   Avg interactions per user: {len(filtered) / filtered['user_id'].nunique():.2f}")
        
        return filtered
    
    def create_interaction_matrix(
        self,
        interactions_df: pd.DataFrame
    ) -> Tuple[csr_matrix, Dict, Dict]:
        """
        Create sparse interaction matrix for collaborative filtering
        
        Args:
            interactions_df: DataFrame with user_id, property_id, weight
            
        Returns:
            Sparse matrix, user_id_map, property_id_map
        """
        # Create mappings
        unique_users = interactions_df['user_id'].unique()
        unique_properties = interactions_df['property_id'].unique()
        
        self.user_id_map = {uid: idx for idx, uid in enumerate(unique_users)}
        self.property_id_map = {pid: idx for idx, pid in enumerate(unique_properties)}
        
        self.reverse_user_map = {idx: uid for uid, idx in self.user_id_map.items()}
        self.reverse_property_map = {idx: pid for pid, idx in self.property_id_map.items()}
        
        # Map IDs to indices
        interactions_df['user_idx'] = interactions_df['user_id'].map(self.user_id_map)
        interactions_df['property_idx'] = interactions_df['property_id'].map(self.property_id_map)
        
        # Create sparse matrix
        rows = interactions_df['user_idx'].values
        cols = interactions_df['property_idx'].values
        data = interactions_df['weight'].values
        
        interaction_matrix = csr_matrix(
            (data, (rows, cols)),
            shape=(len(self.user_id_map), len(self.property_id_map))
        )
        
        logger.info(f"✅ Created sparse matrix: {interaction_matrix.shape}")
        logger.info(f"   Sparsity: {100 * (1 - interaction_matrix.nnz / (interaction_matrix.shape[0] * interaction_matrix.shape[1])):.2f}%")
        
        return interaction_matrix, self.user_id_map, self.property_id_map
    
    def extract_property_features(self) -> pd.DataFrame:
        """Extract property features for content-based filtering"""
        conn = self._get_db_connection()
        
        query = """
            SELECT 
                property_id,
                property_type,
                location_city,
                location_state,
                bedrooms,
                bathrooms,
                max_guests,
                COALESCE(array_length(amenities, 1), 0) as amenities_count,
                COALESCE(property_rating, 0) as rating,
                base_price,
                instant_bookable,
                host_is_superhost,
                amenities
            FROM silver.properties
            WHERE is_active = TRUE
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def extract_user_preferences(self) -> pd.DataFrame:
        """Extract user preferences from booking history"""
        conn = self._get_db_connection()
        
        query = """
            SELECT 
                b.guest_id as user_id,
                AVG(p.bedrooms) as avg_bedrooms_booked,
                AVG(p.bathrooms) as avg_bathrooms_booked,
                AVG(p.base_price) as avg_price_paid,
                AVG(b.nights) as avg_nights_stayed,
                MODE() WITHIN GROUP (ORDER BY p.property_type) as preferred_property_type,
                MODE() WITHIN GROUP (ORDER BY p.location_city) as preferred_city,
                COUNT(*) as total_bookings,
                COUNT(DISTINCT p.location_city) as cities_visited
            FROM silver.bookings b
            JOIN silver.properties p ON b.property_id = p.property_id
            WHERE b.booking_status IN ('confirmed', 'completed')
            GROUP BY b.guest_id
            HAVING COUNT(*) >= 2
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def create_train_test_split(
        self,
        interactions_df: pd.DataFrame,
        test_size: float = 0.2,
        random_state: int = 42
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split interactions into train and test sets
        Ensures each user has interactions in both sets
        
        Args:
            interactions_df: Full interaction data
            test_size: Fraction for test set
            random_state: Random seed
            
        Returns:
            train_df, test_df
        """
        np.random.seed(random_state)
        
        train_list = []
        test_list = []
        
        for user_id in interactions_df['user_id'].unique():
            user_data = interactions_df[interactions_df['user_id'] == user_id]
            
            n_test = max(1, int(len(user_data) * test_size))
            
            test_indices = np.random.choice(
                user_data.index,
                size=n_test,
                replace=False
            )
            
            test_list.append(user_data.loc[test_indices])
            train_list.append(user_data.drop(test_indices))
        
        train_df = pd.concat(train_list, ignore_index=True)
        test_df = pd.concat(test_list, ignore_index=True)
        
        logger.info(f"📊 Train/Test Split:")
        logger.info(f"   Train: {len(train_df)} interactions")
        logger.info(f"   Test: {len(test_df)} interactions")
        
        return train_df, test_df


# Example usage
if __name__ == "__main__":
    processor = RecommendationDataProcessor()
    
    # Extract interactions
    interactions = processor.extract_interaction_data(
        min_interactions=3,
        lookback_days=180
    )
    
    # Create matrix
    matrix, user_map, property_map = processor.create_interaction_matrix(interactions)
    
    print(f"\n📊 Interaction Matrix: {matrix.shape}")
    print(f"Users: {len(user_map)}, Properties: {len(property_map)}")