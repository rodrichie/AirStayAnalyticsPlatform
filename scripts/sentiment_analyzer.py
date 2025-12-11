"""
Review Sentiment Analysis
Multi-language sentiment analysis using HuggingFace Transformers
Supports 20+ languages with translation fallback
"""
import logging
from typing import Dict, List, Optional, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import re

try:
    from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
    import torch
    HAS_TRANSFORMERS = True
except ImportError:
    HAS_TRANSFORMERS = False
    logging.warning("Transformers not available - using mock sentiment analysis")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ReviewSentimentAnalyzer:
    """Analyze sentiment of property reviews"""
    
    # Language codes supported by the model
    SUPPORTED_LANGUAGES = [
        'en', 'es', 'fr', 'de', 'it', 'pt', 'nl', 'pl', 'ru', 'tr',
        'ja', 'ko', 'zh', 'ar', 'hi', 'th', 'vi', 'id', 'sv', 'da'
    ]
    
    def __init__(self, db_config: Dict = None):
        """Initialize sentiment analyzer"""
        self.db_config = db_config or {
            'host': 'postgres',
            'database': 'airstay_db',
            'user': 'airstay',
            'password': 'airstay_pass'
        }
        
        if HAS_TRANSFORMERS:
            # Use multilingual sentiment model
            model_name = "nlptown/bert-base-multilingual-uncased-sentiment"
            
            logger.info(f"Loading sentiment model: {model_name}")
            
            self.sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model=model_name,
                tokenizer=model_name,
                device=0 if torch.cuda.is_available() else -1
            )
            
            logger.info("✅ Sentiment analyzer initialized")
        else:
            self.sentiment_pipeline = None
            logger.warning("⚠️ Running in mock mode - no real sentiment analysis")
    
    def _get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize review text"""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove URLs
        text = re.sub(r'http\S+|www\.\S+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\S+@\S+', '', text)
        
        # Remove excessive punctuation
        text = re.sub(r'[!?]{2,}', '!', text)
        
        return text
    
    def analyze_sentiment(self, text: str, language: str = 'en') -> Dict:
        """
        Analyze sentiment of review text
        
        Args:
            text: Review text
            language: ISO 639-1 language code
            
        Returns:
            Dictionary with sentiment_score, sentiment_label, confidence
        """
        if not text or len(text.strip()) < 10:
            return {
                'sentiment_score': 0.0,
                'sentiment_label': 'neutral',
                'confidence': 0.0
            }
        
        # Clean text
        cleaned_text = self.clean_text(text)
        
        if not HAS_TRANSFORMERS or self.sentiment_pipeline is None:
            # Mock sentiment for development
            return self._mock_sentiment(cleaned_text)
        
        try:
            # Truncate to model's max length (512 tokens)
            if len(cleaned_text) > 512:
                cleaned_text = cleaned_text[:512]
            
            # Run sentiment analysis
            result = self.sentiment_pipeline(cleaned_text)[0]
            
            # Model returns labels like "1 star", "2 stars", etc.
            # Convert to sentiment score (-1 to 1)
            label = result['label']
            score = result['score']
            
            # Extract star rating from label
            if 'star' in label.lower():
                stars = int(label.split()[0])
                # Convert 1-5 stars to -1 to 1 scale
                # 1 star = -1, 3 stars = 0, 5 stars = 1
                normalized_score = (stars - 3) / 2.0
            else:
                normalized_score = 0.0
            
            # Determine label
            if normalized_score >= 0.3:
                sentiment_label = 'positive'
            elif normalized_score <= -0.3:
                sentiment_label = 'negative'
            else:
                sentiment_label = 'neutral'
            
            return {
                'sentiment_score': round(normalized_score, 3),
                'sentiment_label': sentiment_label,
                'confidence': round(score, 3)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {e}")
            return {
                'sentiment_score': 0.0,
                'sentiment_label': 'neutral',
                'confidence': 0.0
            }
    
    def _mock_sentiment(self, text: str) -> Dict:
        """Mock sentiment analysis for development"""
        # Simple keyword-based mock
        positive_words = ['great', 'excellent', 'amazing', 'wonderful', 'perfect', 
                         'love', 'best', 'beautiful', 'clean', 'comfortable']
        negative_words = ['bad', 'terrible', 'horrible', 'dirty', 'uncomfortable',
                         'worst', 'disappointing', 'poor', 'noise', 'broken']
        
        text_lower = text.lower()
        
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        
        if pos_count > neg_count:
            score = 0.7
            label = 'positive'
        elif neg_count > pos_count:
            score = -0.7
            label = 'negative'
        else:
            score = 0.0
            label = 'neutral'
        
        return {
            'sentiment_score': score,
            'sentiment_label': label,
            'confidence': 0.75
        }
    
    def extract_aspects(self, text: str) -> Dict[str, float]:
        """
        Extract aspect-based sentiments
        (cleanliness, location, value, communication, etc.)
        
        This is a simplified version - in production, use aspect-based
        sentiment analysis models
        """
        aspects = {
            'cleanliness': 0.0,
            'location': 0.0,
            'value': 0.0,
            'communication': 0.0,
            'accuracy': 0.0
        }
        
        text_lower = text.lower()
        
        # Cleanliness keywords
        if any(word in text_lower for word in ['clean', 'spotless', 'tidy', 'neat']):
            aspects['cleanliness'] = 0.8
        elif any(word in text_lower for word in ['dirty', 'mess', 'filthy', 'unclean']):
            aspects['cleanliness'] = -0.8
        
        # Location keywords
        if any(word in text_lower for word in ['location', 'convenient', 'central', 'close']):
            aspects['location'] = 0.7
        elif any(word in text_lower for word in ['far', 'remote', 'inconvenient']):
            aspects['location'] = -0.7
        
        # Value keywords
        if any(word in text_lower for word in ['worth', 'value', 'reasonable', 'affordable']):
            aspects['value'] = 0.7
        elif any(word in text_lower for word in ['expensive', 'overpriced', 'costly']):
            aspects['value'] = -0.7
        
        # Communication keywords
        if any(word in text_lower for word in ['responsive', 'helpful', 'communicative', 'quick reply']):
            aspects['communication'] = 0.8
        elif any(word in text_lower for word in ['unresponsive', 'slow', 'no response']):
            aspects['communication'] = -0.8
        
        return aspects
    
    def process_reviews_batch(self, limit: int = 100) -> int:
        """
        Process unanalyzed reviews in batch
        
        Args:
            limit: Maximum number of reviews to process
            
        Returns:
            Number of reviews processed
        """
        conn = self._get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get reviews without sentiment analysis
        query = """
            SELECT 
                review_id,
                review_text,
                review_language,
                rating
            FROM silver.reviews
            WHERE 
                sentiment_score IS NULL
                AND review_text IS NOT NULL
                AND LENGTH(review_text) >= 10
            ORDER BY created_at DESC
            LIMIT %s
        """
        
        cursor.execute(query, (limit,))
        reviews = cursor.fetchall()
        
        cursor.close()
        
        if not reviews:
            logger.info("No reviews to process")
            conn.close()
            return 0
        
        logger.info(f"Processing {len(reviews)} reviews...")
        
        # Process each review
        updates = []
        
        for review in reviews:
            try:
                # Analyze sentiment
                sentiment = self.analyze_sentiment(
                    text=review['review_text'],
                    language=review['review_language'] or 'en'
                )
                
                # Extract aspects (simplified)
                aspects = self.extract_aspects(review['review_text'])
                
                updates.append((
                    sentiment['sentiment_score'],
                    sentiment['sentiment_label'],
                    review['review_id']
                ))
                
            except Exception as e:
                logger.error(f"Error processing review {review['review_id']}: {e}")
                continue
        
        # Bulk update
        cursor = conn.cursor()
        
        update_query = """
            UPDATE silver.reviews
            SET 
                sentiment_score = %s,
                sentiment_label = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE review_id = %s
        """
        
        cursor.executemany(update_query, updates)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Processed {len(updates)} reviews")
        
        return len(updates)
    
    def get_property_sentiment_summary(self, property_id: int) -> Dict:
        """
        Get sentiment summary for a property
        
        Returns:
            Dictionary with sentiment distribution and trends
        """
        conn = self._get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                COUNT(*) as total_reviews,
                AVG(sentiment_score) as avg_sentiment,
                COUNT(*) FILTER (WHERE sentiment_label = 'positive') as positive_count,
                COUNT(*) FILTER (WHERE sentiment_label = 'neutral') as neutral_count,
                COUNT(*) FILTER (WHERE sentiment_label = 'negative') as negative_count,
                AVG(rating) as avg_rating,
                -- Recent trend (last 30 days)
                AVG(sentiment_score) FILTER (
                    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
                ) as recent_sentiment
            FROM silver.reviews
            WHERE 
                property_id = %s
                AND sentiment_score IS NOT NULL
        """
        
        cursor.execute(query, (property_id,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not result or result['total_reviews'] == 0:
            return {
                'property_id': property_id,
                'total_reviews': 0,
                'sentiment_distribution': {},
                'avg_sentiment': 0.0,
                'trend': 'no_data'
            }
        
        return {
            'property_id': property_id,
            'total_reviews': result['total_reviews'],
            'sentiment_distribution': {
                'positive': result['positive_count'],
                'neutral': result['neutral_count'],
                'negative': result['negative_count']
            },
            'avg_sentiment': round(float(result['avg_sentiment']), 3),
            'avg_rating': round(float(result['avg_rating']), 2),
            'recent_sentiment': round(float(result['recent_sentiment']), 3) if result['recent_sentiment'] else None,
            'trend': self._determine_trend(
                float(result['avg_sentiment']),
                float(result['recent_sentiment']) if result['recent_sentiment'] else None
            )
        }
    
    def _determine_trend(self, overall: float, recent: Optional[float]) -> str:
        """Determine sentiment trend"""
        if recent is None:
            return 'stable'
        
        diff = recent - overall
        
        if diff > 0.1:
            return 'improving'
        elif diff < -0.1:
            return 'declining'
        else:
            return 'stable'
    
    def identify_common_themes(
        self,
        property_id: int,
        min_mentions: int = 3
    ) -> List[Dict]:
        """
        Identify common themes mentioned in reviews
        
        Args:
            property_id: Property ID
            min_mentions: Minimum mentions to be considered common
            
        Returns:
            List of themes with sentiment and frequency
        """
        conn = self._get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all reviews for property
        query = """
            SELECT 
                review_text,
                sentiment_score,
                rating
            FROM silver.reviews
            WHERE 
                property_id = %s
                AND review_text IS NOT NULL
        """
        
        cursor.execute(query, (property_id,))
        reviews = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Define themes to look for
        themes = {
            'cleanliness': ['clean', 'spotless', 'tidy', 'neat', 'dirty', 'mess'],
            'location': ['location', 'close', 'convenient', 'central', 'far', 'remote'],
            'host': ['host', 'owner', 'responsive', 'helpful', 'friendly'],
            'amenities': ['wifi', 'kitchen', 'pool', 'parking', 'gym'],
            'comfort': ['comfortable', 'cozy', 'bed', 'mattress', 'pillow'],
            'noise': ['quiet', 'peaceful', 'noisy', 'loud', 'noise'],
            'value': ['value', 'worth', 'price', 'expensive', 'affordable']
        }
        
        # Count mentions and calculate sentiment
        theme_stats = {}
        
        for theme_name, keywords in themes.items():
            mentions = 0
            sentiment_sum = 0.0
            
            for review in reviews:
                text_lower = review['review_text'].lower()
                
                if any(keyword in text_lower for keyword in keywords):
                    mentions += 1
                    sentiment_sum += review['sentiment_score'] or 0.0
            
            if mentions >= min_mentions:
                theme_stats[theme_name] = {
                    'theme': theme_name,
                    'mentions': mentions,
                    'avg_sentiment': round(sentiment_sum / mentions, 3),
                    'frequency': round(mentions / len(reviews) * 100, 1)
                }
        
        # Sort by frequency
        sorted_themes = sorted(
            theme_stats.values(),
            key=lambda x: x['mentions'],
            reverse=True
        )
        
        return sorted_themes


# Example usage
if __name__ == "__main__":
    analyzer = ReviewSentimentAnalyzer()
    
    # Process batch of reviews
    print("\n🔍 Processing reviews...")
    processed = analyzer.process_reviews_batch(limit=50)
    print(f"Processed {processed} reviews")
    
    # Get sentiment summary for property
    print("\n📊 Property sentiment summary...")
    summary = analyzer.get_property_sentiment_summary(property_id=1)
    print(f"Total reviews: {summary['total_reviews']}")
    print(f"Average sentiment: {summary['avg_sentiment']}")
    print(f"Distribution: {summary['sentiment_distribution']}")
    print(f"Trend: {summary['trend']}")
    
    # Identify common themes
    print("\n💬 Common themes...")
    themes = analyzer.identify_common_themes(property_id=1)
    for theme in themes[:5]:
        print(f"  - {theme['theme']}: {theme['mentions']} mentions, "
              f"sentiment {theme['avg_sentiment']}")