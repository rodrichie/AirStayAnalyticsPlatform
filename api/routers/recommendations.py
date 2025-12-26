"""
Recommendation Endpoints
ML-powered property recommendations for users
"""
import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy import text
import pickle

from api.dependencies import get_db, cache_response
from api.schemas import RecommendationRequest, RecommendationResponse
from api.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/recommendations", tags=["Recommendations"])

# Load recommendation model (lazy loading)
_recommendation_model = None


def get_recommendation_model():
    """Load recommendation model singleton"""
    global _recommendation_model
    
    if _recommendation_model is None:
        try:
            with open(settings.RECOMMENDATION_MODEL_PATH, 'rb') as f:
                _recommendation_model = pickle.load(f)
            logger.info("✅ Recommendation model loaded")
        except Exception as e:
            logger.error(f"Failed to load recommendation model: {e}")
            _recommendation_model = None
    
    return _recommendation_model


@router.get("/user/{user_id}", response_model=List[RecommendationResponse])
@cache_response(ttl=600)  # 10 minute cache
async def get_user_recommendations(
    user_id: int,
    n_recommendations: int = Query(10, ge=1, le=50),
    city: Optional[str] = Query(None),
    min_price: Optional[float] = Query(None),
    max_price: Optional[float] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get personalized property recommendations for user
    
    Based on:
    - User's booking history
    - Property ratings and features
    - Collaborative filtering
    - Location preferences
    """
    logger.info(f"Getting recommendations for user {user_id}")
    
    # Try ML model first
    model = get_recommendation_model()
    
    if model and hasattr(model, 'recommend_for_user'):
        try:
            from ml.models.recommendation_model import PropertyRecommendationModel
            
            recommendations = model.recommend_for_user(
                user_id=user_id,
                n_recommendations=n_recommendations,
                filter_city=city,
                min_price=min_price,
                max_price=max_price
            )
            
            return recommendations
            
        except Exception as e:
            logger.warning(f"ML recommendation failed: {e}")
    
    # Fallback: popularity-based recommendations
    logger.info("Using popularity-based fallback recommendations")
    
    # Get user's past bookings to understand preferences
    user_prefs_query = text("""
        WITH user_bookings AS (
            SELECT 
                p.property_type,
                p.location_city,
                AVG(p.bedrooms) as avg_bedrooms,
                AVG(p.base_price) as avg_price
            FROM silver.bookings b
            JOIN silver.properties p ON b.property_id = p.property_id
            WHERE b.guest_id = :user_id
            AND b.booking_status IN ('confirmed', 'completed')
            GROUP BY p.property_type, p.location_city
            LIMIT 1
        )
        SELECT 
            p.property_id,
            p.listing_id,
            p.title,
            p.property_type,
            p.location_city as city,
            p.bedrooms,
            p.bathrooms,
            p.max_guests,
            p.base_price,
            COALESCE(p.property_rating, 0) as rating,
            COALESCE(p.review_count, 0) as review_count,
            p.property_images[1] as image_url,
            0.7 as recommendation_score,
            'Based on your booking history' as recommendation_reason
        FROM silver.properties p
        LEFT JOIN user_bookings ub ON TRUE
        WHERE 
            p.is_active = TRUE
            AND p.property_id NOT IN (
                SELECT property_id FROM silver.bookings 
                WHERE guest_id = :user_id
            )
            AND (:city IS NULL OR p.location_city = :city)
            AND (:min_price IS NULL OR p.base_price >= :min_price)
            AND (:max_price IS NULL OR p.base_price <= :max_price)
            AND (
                ub.property_type IS NULL OR
                p.property_type = ub.property_type OR
                p.location_city = ub.location_city
            )
        ORDER BY 
            CASE WHEN p.location_city = ub.location_city THEN 1 ELSE 2 END,
            p.property_rating DESC NULLS LAST,
            p.review_count DESC
        LIMIT :limit
    """)
    
    result = db.execute(user_prefs_query, {
        'user_id': user_id,
        'city': city,
        'min_price': min_price,
        'max_price': max_price,
        'limit': n_recommendations
    })
    
    # If no user history, just return popular properties
    recommendations = [dict(row._mapping) for row in result]
    
    if len(recommendations) == 0:
        # Pure popularity fallback
        popular_query = text("""
            SELECT 
                p.property_id,
                p.listing_id,
                p.title,
                p.property_type,
                p.location_city as city,
                p.bedrooms,
                p.bathrooms,
                p.max_guests,
                p.base_price,
                COALESCE(p.property_rating, 0) as rating,
                COALESCE(p.review_count, 0) as review_count,
                p.property_images[1] as image_url,
                0.5 as recommendation_score,
                'Popular property' as recommendation_reason
            FROM silver.properties p
            WHERE 
                p.is_active = TRUE
                AND (:city IS NULL OR p.location_city = :city)
                AND (:min_price IS NULL OR p.base_price >= :min_price)
                AND (:max_price IS NULL OR p.base_price <= :max_price)
            ORDER BY 
                p.review_count DESC,
                p.property_rating DESC NULLS LAST
            LIMIT :limit
        """)
        
        result = db.execute(popular_query, {
            'city': city,
            'min_price': min_price,
            'max_price': max_price,
            'limit': n_recommendations
        })
        
        recommendations = [dict(row._mapping) for row in result]
    
    return recommendations


@router.get("/property/{property_id}/similar", response_model=List[RecommendationResponse])
@cache_response(ttl=600)
async def get_similar_property_recommendations(
    property_id: int,
    n_recommendations: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """
    Get similar property recommendations
    
    Based on:
    - Property features
    - Location
    - Price range
    - Property type
    """
    # Try ML model
    model = get_recommendation_model()
    
    if model and hasattr(model, 'recommend_similar_properties'):
        try:
            recommendations = model.recommend_similar_properties(
                property_id=property_id,
                n_recommendations=n_recommendations
            )
            
            return recommendations
            
        except Exception as e:
            logger.warning(f"ML similarity failed: {e}")
    
    # Fallback: feature-based similarity
    query = text("""
        WITH reference_property AS (
            SELECT 
                property_type,
                location_city,
                bedrooms,
                base_price,
                latitude,
                longitude
            FROM silver.properties
            WHERE property_id = :property_id
        )
        SELECT 
            p.property_id,
            p.listing_id,
            p.title,
            p.property_type,
            p.location_city as city,
            p.bedrooms,
            p.bathrooms,
            p.max_guests,
            p.base_price,
            COALESCE(p.property_rating, 0) as rating,
            COALESCE(p.review_count, 0) as review_count,
            p.property_images[1] as image_url,
            (
                CASE WHEN p.property_type = rp.property_type THEN 0.3 ELSE 0 END +
                CASE WHEN p.bedrooms = rp.bedrooms THEN 0.3 ELSE 0 END +
                CASE WHEN ABS(p.base_price - rp.base_price) < rp.base_price * 0.3 THEN 0.2 ELSE 0 END +
                0.2
            ) as recommendation_score,
            'Similar property' as recommendation_reason
        FROM silver.properties p
        CROSS JOIN reference_property rp
        WHERE 
            p.is_active = TRUE
            AND p.property_id != :property_id
            AND p.location_city = rp.location_city
            AND ST_DWithin(
                p.location_point,
                ST_SetSRID(ST_MakePoint(rp.longitude, rp.latitude), 4326)::geography,
                5000
            )
        ORDER BY recommendation_score DESC
        LIMIT :limit
    """)
    
    result = db.execute(query, {
        'property_id': property_id,
        'limit': n_recommendations
    })
    
    return [dict(row._mapping) for row in result]


@router.get("/trending")
@cache_response(ttl=1800)  # 30 minute cache
async def get_trending_destinations(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """
    Get trending destinations based on search and booking activity
    """
    query = text("""
        SELECT 
            city,
            SUM(search_count) as total_searches,
            AVG(unique_users) as avg_daily_users,
            COUNT(DISTINCT metric_date) as days_tracked
        FROM silver.search_trends_cities
        WHERE window_start >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY city
        HAVING SUM(search_count) > 10
        ORDER BY total_searches DESC
        LIMIT :limit
    """)
    
    result = db.execute(query, {'limit': limit})
    
    trending = [
        {
            'city': row.city,
            'search_volume': int(row.total_searches),
            'avg_daily_users': int(row.avg_daily_users),
            'trend': 'rising'
        }
        for row in result
    ]
    
    return {'trending_destinations': trending}