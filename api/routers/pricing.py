"""
Pricing Endpoints
Dynamic pricing recommendations and forecasts
"""
import logging
from typing import List
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy import text
import pickle

from api.dependencies import get_db, cache_response
from api.schemas import PriceRecommendation
from api.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/pricing", tags=["Pricing"])

# Load pricing model (lazy loading)
_pricing_model = None


def get_pricing_model():
    """Load pricing model singleton"""
    global _pricing_model
    
    if _pricing_model is None:
        try:
            with open(settings.PRICING_MODEL_PATH, 'rb') as f:
                _pricing_model = pickle.load(f)
            logger.info("✅ Pricing model loaded")
        except Exception as e:
            logger.error(f"Failed to load pricing model: {e}")
            _pricing_model = None
    
    return _pricing_model


@router.get("/{property_id}/recommend", response_model=PriceRecommendation)
@cache_response(ttl=3600)  # 1 hour cache
async def get_price_recommendation(
    property_id: int,
    target_date: datetime = Query(None, description="Date to predict for"),
    db: Session = Depends(get_db)
):
    """
    Get ML-based price recommendation for property
    
    Returns optimized price based on:
    - Historical demand
    - Seasonal patterns
    - Competitor pricing
    - Property features
    """
    logger.info(f"Getting price recommendation for property {property_id}")
    
    # Get current price
    price_query = text("""
        SELECT base_price
        FROM silver.properties
        WHERE property_id = :property_id AND is_active = TRUE
    """)
    
    result = db.execute(price_query, {'property_id': property_id})
    current_data = result.first()
    
    if not current_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property {property_id} not found"
        )
    
    current_price = float(current_data.base_price)
    
    # Try to get ML prediction
    model = get_pricing_model()
    
    if model and hasattr(model, 'predict'):
        try:
            # Use actual ML model
            from ml.models.dynamic_pricing_model import DynamicPricingModel
            
            prediction = model.predict(
                property_id=property_id,
                target_date=target_date
            )
            
            return prediction
            
        except Exception as e:
            logger.warning(f"ML prediction failed: {e}")
    
    # Fallback: rule-based recommendation
    # Check recent market prices
    market_query = text("""
        SELECT AVG(base_price) as market_avg
        FROM silver.properties p
        WHERE 
            p.location_city = (
                SELECT location_city FROM silver.properties WHERE property_id = :property_id
            )
            AND p.property_type = (
                SELECT property_type FROM silver.properties WHERE property_id = :property_id
            )
            AND p.is_active = TRUE
    """)
    
    market_result = db.execute(market_query, {'property_id': property_id})
    market_data = market_result.first()
    
    market_avg = float(market_data.market_avg) if market_data else current_price
    
    # Simple recommendation: move toward market average
    recommended_price = current_price * 0.7 + market_avg * 0.3
    price_change = recommended_price - current_price
    price_change_pct = (price_change / current_price) * 100
    
    return {
        'property_id': property_id,
        'current_price': current_price,
        'recommended_price': round(recommended_price, 2),
        'price_change': round(price_change, 2),
        'price_change_pct': round(price_change_pct, 2),
        'confidence_interval_lower': round(recommended_price * 0.95, 2),
        'confidence_interval_upper': round(recommended_price * 1.05, 2),
        'recommendation_reason': 'Market-based adjustment (ML model unavailable)'
    }


@router.get("/recommendations", response_model=List[PriceRecommendation])
@cache_response(ttl=3600)
async def get_batch_recommendations(
    city: str = Query(None, description="Filter by city"),
    min_change_pct: float = Query(5.0, description="Minimum price change %"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db)
):
    """
    Get batch price recommendations
    
    Returns properties with significant price change recommendations
    """
    query = text("""
        SELECT 
            property_id,
            recommendation_date,
            current_price,
            recommended_price,
            price_change_pct,
            reason,
            confidence_score
        FROM gold.pricing_recommendations
        WHERE 
            recommendation_date >= CURRENT_DATE - INTERVAL '7 days'
            AND ABS(price_change_pct) >= :min_change_pct
            AND (:city IS NULL OR property_id IN (
                SELECT property_id FROM silver.properties WHERE location_city = :city
            ))
        ORDER BY ABS(price_change_pct) DESC
        LIMIT :limit
    """)
    
    result = db.execute(query, {
        'city': city,
        'min_change_pct': min_change_pct,
        'limit': limit
    })
    
    recommendations = [
        {
            'property_id': row.property_id,
            'current_price': float(row.current_price),
            'recommended_price': float(row.recommended_price),
            'price_change': float(row.recommended_price - row.current_price),
            'price_change_pct': float(row.price_change_pct),
            'confidence_interval_lower': float(row.recommended_price * 0.95),
            'confidence_interval_upper': float(row.recommended_price * 1.05),
            'recommendation_reason': row.reason
        }
        for row in result
    ]
    
    return recommendations