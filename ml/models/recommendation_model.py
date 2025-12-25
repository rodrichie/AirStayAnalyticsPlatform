"""
Recommendation Engine using LightFM
Hybrid collaborative + content-based filtering
"""
import logging
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
import pickle

from lightfm import LightFM
from lightfm.evaluation import precision_at_k, auc_score, recall_at_k
from lightfm.data import Dataset

from ml.features.recommendation_features import RecommendationDataProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PropertyRecommendationModel:
    """Hybrid recommendation model for property suggestions"""
    
    def __init__(self, data_processor: RecommendationDataProcessor = None):
        """Initialize recommendation model"""
        self.data_processor = data_processor or RecommendationDataProcessor()
        self.model = None
        self.dataset = None
        self.user_features_matrix = None
        self.item_features_matrix = None
        self.training_metrics = {}
        
        logger.info("✅ Recommendation Model initialized")
    
    def prepare_data(
        self,
        interactions_df: pd.DataFrame,
        property_features_df: pd.DataFrame = None
    ):
        """
        Prepare data for LightFM
        
        Args:
            interactions_df: User-property interactions
            property_features_df: Property features for hybrid filtering
        """
        logger.info("📊 Preparing recommendation data...")
        
        # Create LightFM Dataset
        self.dataset = Dataset()
        
        # Fit users and items
        self.dataset.fit(
            users=interactions_df['user_id'].unique(),
            items=interactions_df['property_id'].unique()
        )
        
        # Build interactions matrix
        (interactions, weights) = self.dataset.build_interactions(
            ((row['user_id'], row['property_id'], row['weight'])
             for idx, row in interactions_df.iterrows())
        )
        
        self.interactions_matrix = interactions
        
        # Build item features if provided
        if property_features_df is not None:
            self._build_item_features(property_features_df)
        
        logger.info(f"✅ Data prepared: {interactions.shape}")
    
    def _build_item_features(self, property_features_df: pd.DataFrame):
        """Build item (property) feature matrix"""
        # Define features
        feature_list = []
        
        for _, row in property_features_df.iterrows():
            property_id = row['property_id']
            features = [
                f"type:{row['property_type']}",
                f"city:{row['location_city']}",
                f"bedrooms:{row['bedrooms']}",
                f"price_range:{self._get_price_range(row['base_price'])}",
                f"rating_tier:{self._get_rating_tier(row['rating'])}",
            ]
            
            # Add amenities as features
            if row['amenities'] and len(row['amenities']) > 0:
                for amenity in row['amenities'][:10]:  # Top 10 amenities
                    features.append(f"amenity:{amenity}")
            
            # Boolean features
            if row['instant_bookable']:
                features.append("instant_bookable")
            if row['host_is_superhost']:
                features.append("superhost")
            
            feature_list.append((property_id, features))
        
        # Fit item features
        all_features = set()
        for _, features in feature_list:
            all_features.update(features)
        
        self.dataset.fit_partial(items=None, item_features=all_features)
        
        # Build feature matrix
        self.item_features_matrix = self.dataset.build_item_features(feature_list)
        
        logger.info(f"✅ Built item features: {self.item_features_matrix.shape}")
    
    def _get_price_range(self, price: float) -> str:
        """Categorize price into ranges"""
        if price < 100:
            return "budget"
        elif price < 200:
            return "mid-range"
        elif price < 400:
            return "upscale"
        else:
            return "luxury"
    
    def _get_rating_tier(self, rating: float) -> str:
        """Categorize rating into tiers"""
        if rating >= 4.5:
            return "excellent"
        elif rating >= 4.0:
            return "very_good"
        elif rating >= 3.5:
            return "good"
        else:
            return "fair"
    
    def train(
        self,
        interactions_train: csr_matrix,
        interactions_test: csr_matrix = None,
        epochs: int = 30,
        num_threads: int = 4,
        loss: str = 'warp',
        learning_rate: float = 0.05,
        item_alpha: float = 0.0,
        user_alpha: float = 0.0
    ):
        """
        Train LightFM model
        
        Args:
            interactions_train: Training interaction matrix
            interactions_test: Test interaction matrix
            epochs: Number of training epochs
            num_threads: Number of parallel threads
            loss: Loss function (warp, bpr, logistic)
            learning_rate: Learning rate
            item_alpha: L2 penalty on item features
            user_alpha: L2 penalty on user features
        """
        logger.info("🚀 Training recommendation model...")
        
        # Initialize model
        self.model = LightFM(
            no_components=30,  # Latent dimensionality
            loss=loss,
            learning_rate=learning_rate,
            item_alpha=item_alpha,
            user_alpha=user_alpha,
            random_state=42
        )
        
        # Train
        for epoch in range(epochs):
            self.model.fit_partial(
                interactions_train,
                item_features=self.item_features_matrix,
                epochs=1,
                num_threads=num_threads
            )
            
            # Evaluate every 5 epochs
            if interactions_test is not None and (epoch + 1) % 5 == 0:
                train_precision = precision_at_k(
                    self.model,
                    interactions_train,
                    item_features=self.item_features_matrix,
                    k=10
                ).mean()
                
                test_precision = precision_at_k(
                    self.model,
                    interactions_test,
                    item_features=self.item_features_matrix,
                    k=10
                ).mean()
                
                logger.info(
                    f"Epoch {epoch + 1}/{epochs} - "
                    f"Train P@10: {train_precision:.4f}, "
                    f"Test P@10: {test_precision:.4f}"
                )
        
        logger.info("✅ Model training completed")
    
    def evaluate(
        self,
        interactions_test: csr_matrix,
        k_values: List[int] = [5, 10, 20]
    ) -> Dict:
        """
        Evaluate model performance
        
        Args:
            interactions_test: Test interaction matrix
            k_values: Values of k for precision@k and recall@k
            
        Returns:
            Dictionary of metrics
        """
        logger.info("📊 Evaluating model...")
        
        metrics = {}
        
        for k in k_values:
            precision = precision_at_k(
                self.model,
                interactions_test,
                item_features=self.item_features_matrix,
                k=k
            ).mean()
            
            recall = recall_at_k(
                self.model,
                interactions_test,
                item_features=self.item_features_matrix,
                k=k
            ).mean()
            
            metrics[f'precision@{k}'] = round(precision, 4)
            metrics[f'recall@{k}'] = round(recall, 4)
        
        # AUC score
        auc = auc_score(
            self.model,
            interactions_test,
            item_features=self.item_features_matrix
        ).mean()
        
        metrics['auc'] = round(auc, 4)
        
        self.training_metrics = metrics
        
        logger.info(f"📈 Model Metrics: {metrics}")
        
        return metrics
    
    def recommend_for_user(
        self,
        user_id: int,
        n_recommendations: int = 10,
        exclude_known: bool = True,
        filter_city: str = None,
        min_price: float = None,
        max_price: float = None
    ) -> List[Dict]:
        """
        Generate property recommendations for a user
        
        Args:
            user_id: User ID
            n_recommendations: Number of recommendations
            exclude_known: Exclude properties user has interacted with
            filter_city: Filter by city
            min_price: Minimum price filter
            max_price: Maximum price filter
            
        Returns:
            List of recommended properties with scores
        """
        if self.model is None:
            raise ValueError("Model not trained")
        
        # Get user index
        user_idx = self.data_processor.user_id_map.get(user_id)
        if user_idx is None:
            logger.warning(f"User {user_id} not in training data")
            return self._recommend_popular(n_recommendations, filter_city)
        
        # Get all property indices
        n_items = len(self.data_processor.property_id_map)
        
        # Predict scores for all properties
        scores = self.model.predict(
            user_idx,
            np.arange(n_items),
            item_features=self.item_features_matrix
        )
        
        # Get top N
        top_indices = np.argsort(-scores)[:n_recommendations * 3]  # Get extra for filtering
        
        # Convert to property IDs and get details
        recommendations = []
        
        conn = self.data_processor._get_db_connection()
        
        for idx in top_indices:
            property_id = self.data_processor.reverse_property_map[idx]
            score = scores[idx]
            
            # Get property details
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    property_id,
                    listing_id,
                    title,
                    property_type,
                    location_city,
                    bedrooms,
                    bathrooms,
                    base_price,
                    property_rating,
                    review_count,
                    property_images[1] as primary_image
                FROM silver.properties
                WHERE property_id = %s AND is_active = TRUE
            """, (property_id,))
            
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                # Apply filters
                if filter_city and result[4] != filter_city:
                    continue
                if min_price and result[7] < min_price:
                    continue
                if max_price and result[7] > max_price:
                    continue
                
                recommendations.append({
                    'property_id': result[0],
                    'listing_id': result[1],
                    'title': result[2],
                    'property_type': result[3],
                    'city': result[4],
                    'bedrooms': result[5],
                    'bathrooms': result[6],
                    'base_price': float(result[7]),
                    'rating': float(result[8]) if result[8] else 0,
                    'review_count': result[9],
                    'image_url': result[10],
                    'recommendation_score': float(score),
                    'recommendation_reason': self._get_recommendation_reason(user_id, property_id)
                })
                
                if len(recommendations) >= n_recommendations:
                    break
        
        conn.close()
        
        logger.info(f"✅ Generated {len(recommendations)} recommendations for user {user_id}")
        
        return recommendations
    
    def _recommend_popular(
        self,
        n_recommendations: int = 10,
        filter_city: str = None
    ) -> List[Dict]:
        """Fallback to popular properties for cold-start users"""
        conn = self.data_processor._get_db_connection()
        
        query = """
            SELECT 
                property_id,
                listing_id,
                title,
                property_type,
                location_city,
                bedrooms,
                bathrooms,
                base_price,
                property_rating,
                review_count,
                property_images[1] as primary_image
            FROM silver.properties
            WHERE is_active = TRUE
        """
        
        params = []
        if filter_city:
            query += " AND location_city = %s"
            params.append(filter_city)
        
        query += " ORDER BY review_count DESC, property_rating DESC LIMIT %s"
        params.append(n_recommendations)
        
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        recommendations = [
            {
                'property_id': r[0],
                'listing_id': r[1],
                'title': r[2],
                'property_type': r[3],
                'city': r[4],
                'bedrooms': r[5],
                'bathrooms': r[6],
                'base_price': float(r[7]),
                'rating': float(r[8]) if r[8] else 0,
                'review_count': r[9],
                'image_url': r[10],
                'recommendation_score': 0.5,
                'recommendation_reason': 'Popular property'
            }
            for r in results
        ]
        
        return recommendations
    
    def _get_recommendation_reason(self, user_id: int, property_id: int) -> str:
        """Generate explanation for recommendation"""
        # Simplified - in production, analyze feature contributions
        reasons = [
            "Based on your booking history",
            "Similar to properties you liked",
            "Popular in your preferred location",
            "Matches your typical preferences"
        ]
        return np.random.choice(reasons)
    
    def recommend_similar_properties(
        self,
        property_id: int,
        n_recommendations: int = 10
    ) -> List[Dict]:
        """
        Find similar properties (item-item similarity)
        
        Args:
            property_id: Property ID to find similar to
            n_recommendations: Number of recommendations
            
        Returns:
            List of similar properties
        """
        if self.model is None:
            raise ValueError("Model not trained")
        
        property_idx = self.data_processor.property_id_map.get(property_id)
        if property_idx is None:
            logger.warning(f"Property {property_id} not in training data")
            return []
        
        # Get item embedding
        item_embeddings = self.model.item_embeddings
        
        if self.item_features_matrix is not None:
            # Use item features
            item_biases = self.model.item_biases
            item_repr = self.model.get_item_representations(self.item_features_matrix)
            target_repr = item_repr[1][property_idx]
            
            # Calculate similarity with all items
            similarities = np.dot(item_repr[1], target_repr)
        else:
            # Use embeddings only
            target_embedding = item_embeddings[property_idx]
            similarities = np.dot(item_embeddings, target_embedding)
        
        # Get top similar (exclude self)
        similar_indices = np.argsort(-similarities)[1:n_recommendations + 1]
        
        # Get property details
        recommendations = []
        conn = self.data_processor._get_db_connection()
        
        for idx in similar_indices:
            similar_property_id = self.data_processor.reverse_property_map[idx]
            similarity_score = similarities[idx]
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    property_id, listing_id, title, property_type,
                    location_city, bedrooms, base_price, property_rating
                FROM silver.properties
                WHERE property_id = %s AND is_active = TRUE
            """, (similar_property_id,))
            
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                recommendations.append({
                    'property_id': result[0],
                    'listing_id': result[1],
                    'title': result[2],
                    'property_type': result[3],
                    'city': result[4],
                    'bedrooms': result[5],
                    'base_price': float(result[6]),
                    'rating': float(result[7]) if result[7] else 0,
                    'similarity_score': float(similarity_score)
                })
        
        conn.close()
        
        return recommendations
    
    def save_model(self, filepath: str):
        """Save model to disk"""
        model_data = {
            'model': self.model,
            'dataset': self.dataset,
            'item_features_matrix': self.item_features_matrix,
            'user_id_map': self.data_processor.user_id_map,
            'property_id_map': self.data_processor.property_id_map,
            'reverse_user_map': self.data_processor.reverse_user_map,
            'reverse_property_map': self.data_processor.reverse_property_map,
            'training_metrics': self.training_metrics,
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        logger.info(f"✅ Model saved to {filepath}")
    
    def load_model(self, filepath: str):
        """Load model from disk"""
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data['model']
        self.dataset = model_data['dataset']
        self.item_features_matrix = model_data['item_features_matrix']
        self.data_processor.user_id_map = model_data['user_id_map']
        self.data_processor.property_id_map = model_data['property_id_map']
        self.data_processor.reverse_user_map = model_data['reverse_user_map']
        self.data_processor.reverse_property_map = model_data['reverse_property_map']
        self.training_metrics = model_data['training_metrics']
        
        logger.info(f"✅ Model loaded from {filepath}")


# Example usage
if __name__ == "__main__":
    # Initialize
    processor = RecommendationDataProcessor()
    model = PropertyRecommendationModel(processor)
    
    # Extract data
    interactions = processor.extract_interaction_data(min_interactions=3)
    property_features = processor.extract_property_features()
    
    # Train/test split
    train_df, test_df = processor.create_train_test_split(interactions)
    
    # Prepare data
    model.prepare_data(train_df, property_features)
    
    # Create test matrix
    test_matrix, _, _ = processor.create_interaction_matrix(test_df)
    
    # Train
    model.train(
        interactions_train=model.interactions_matrix,
        interactions_test=test_matrix,
        epochs=30
    )
    
    # Evaluate
    metrics = model.evaluate(test_matrix)
    
    # Save
    model.save_model('/tmp/recommendation_model.pkl')
    
    # Get recommendations
    recommendations = model.recommend_for_user(
        user_id=1001,
        n_recommendations=10,
        filter_city='New York'
    )
    
    print(f"\n🎯 Recommendations for User 1001:")
    for rec in recommendations:
        print(f"  - {rec['title']} (Score: {rec['recommendation_score']:.3f})")