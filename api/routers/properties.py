"""
Property Endpoints
Search, filter, and retrieve property information
"""
import logging
from typing import List, Optional
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy import func, text

from api.dependencies import get_db, get_redis, cache_response, Paginator
from api.schemas import (
    PropertyDetail, PropertySearch, PropertySearchResult,
    PaginatedResponse, SuccessResponse
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/properties", tags=["Properties"])


@router.get("/search", response_model=PaginatedResponse)
@cache_response(ttl=60)
async def search_properties(
    city: Optional[str] = None,
    check_in: Optional[date] = None,
    check_out: Optional[date] = None,
    num_guests: Optional[int] = None,
    min_bedrooms: Optional[int] = None,
    max_price: Optional[float] = None,
    property_type: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    radius_km: Optional[float] = 5.0,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Search for properties with filters
    
    - **city**: Filter by city name
    - **check_in**: Check-in date for availability
    - **check_out**: Check-out date for availability
    - **num_guests**: Minimum guest capacity
    - **min_bedrooms**: Minimum bedrooms
    - **max_price**: Maximum nightly price
    - **property_type**: Filter by property type
    - **latitude/longitude**: Search near coordinates
    - **radius_km**: Search radius in kilometers
    """
    logger.info(f"Property search: city={city}, guests={num_guests}")
    
    # Build query
    query = text("""
        SELECT 
            p.property_id,
            p.listing_id,
            p.title,
            p.property_type,
            p.location_city as city,
            p.neighborhood,
            p.bedrooms,
            p.bathrooms,
            p.max_guests,
            p.base_price,
            COALESCE(array_length(p.amenities, 1), 0) as amenities_count,
            COALESCE(p.property_rating, 0) as rating,
            COALESCE(p.review_count, 0) as review_count,
            p.property_images[1] as primary_image,
            CASE 
                WHEN :latitude IS NOT NULL AND :longitude IS NOT NULL THEN
                    ST_Distance(
                        p.location_point,
                        ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326)::geography
                    ) / 1000.0
                ELSE NULL
            END as distance_km
        FROM silver.properties p
        WHERE p.is_active = TRUE
        AND (:city IS NULL OR p.location_city = :city)
        AND (:num_guests IS NULL OR p.max_guests >= :num_guests)
        AND (:min_bedrooms IS NULL OR p.bedrooms >= :min_bedrooms)
        AND (:max_price IS NULL OR p.base_price <= :max_price)
        AND (:property_type IS NULL OR p.property_type = :property_type)
        AND (
            :latitude IS NULL OR :longitude IS NULL OR
            ST_DWithin(
                p.location_point,
                ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326)::geography,
                :radius_km * 1000
            )
        )
        ORDER BY 
            CASE WHEN :latitude IS NOT NULL THEN distance_km END ASC,
            p.property_rating DESC NULLS LAST,
            p.review_count DESC
        LIMIT :limit OFFSET :offset
    """)
    
    # Get total count
    count_query = text("""
        SELECT COUNT(*) FROM silver.properties p
        WHERE p.is_active = TRUE
        AND (:city IS NULL OR p.location_city = :city)
        AND (:num_guests IS NULL OR p.max_guests >= :num_guests)
        AND (:min_bedrooms IS NULL OR p.bedrooms >= :min_bedrooms)
        AND (:max_price IS NULL OR p.base_price <= :max_price)
        AND (:property_type IS NULL OR p.property_type = :property_type)
    """)
    
    # Pagination
    paginator = Paginator(page=page, page_size=page_size)
    
    # Execute queries
    params = {
        'city': city,
        'num_guests': num_guests,
        'min_bedrooms': min_bedrooms,
        'max_price': max_price,
        'property_type': property_type,
        'latitude': latitude,
        'longitude': longitude,
        'radius_km': radius_km,
        'limit': paginator.limit,
        'offset': paginator.offset
    }
    
    result = db.execute(query, params)
    properties = [dict(row._mapping) for row in result]
    
    count_result = db.execute(count_query, params)
    total_count = count_result.scalar()
    
    return {
        'data': properties,
        'pagination': paginator.get_metadata(total_count)
    }


@router.get("/{property_id}", response_model=PropertyDetail)
@cache_response(ttl=300)
async def get_property(
    property_id: int,
    db: Session = Depends(get_db)
):
    """Get detailed property information"""
    
    query = text("""
        SELECT 
            p.property_id,
            p.listing_id,
            p.title,
            p.description,
            p.property_type,
            p.location_city as city,
            p.neighborhood,
            p.bedrooms,
            p.bathrooms,
            p.beds,
            p.max_guests,
            p.base_price,
            p.cleaning_fee,
            p.latitude,
            p.longitude,
            p.amenities,
            COALESCE(array_length(p.amenities, 1), 0) as amenities_count,
            p.instant_bookable,
            p.minimum_nights,
            p.maximum_nights,
            p.cancellation_policy,
            p.host_is_superhost,
            COALESCE(p.property_rating, 0) as rating,
            COALESCE(p.review_count, 0) as review_count,
            COALESCE(p.quality_score, 0) as quality_score,
            p.property_images
        FROM silver.properties p
        WHERE p.property_id = :property_id
        AND p.is_active = TRUE
    """)
    
    result = db.execute(query, {'property_id': property_id})
    property_data = result.first()
    
    if not property_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property {property_id} not found"
        )
    
    return dict(property_data._mapping)


@router.get("/{property_id}/availability")
@cache_response(ttl=60)
async def get_property_availability(
    property_id: int,
    start_date: date = Query(..., description="Start date"),
    end_date: date = Query(..., description="End date"),
    db: Session = Depends(get_db)
):
    """Get property availability calendar"""
    
    query = text("""
        SELECT 
            calendar_date,
            is_available,
            price,
            minimum_nights
        FROM silver.availability_calendar
        WHERE 
            property_id = :property_id
            AND calendar_date >= :start_date
            AND calendar_date <= :end_date
        ORDER BY calendar_date
    """)
    
    result = db.execute(query, {
        'property_id': property_id,
        'start_date': start_date,
        'end_date': end_date
    })
    
    availability = [
        {
            'date': str(row.calendar_date),
            'available': row.is_available,
            'price': float(row.price),
            'minimum_nights': row.minimum_nights
        }
        for row in result
    ]
    
    return {
        'property_id': property_id,
        'start_date': str(start_date),
        'end_date': str(end_date),
        'availability': availability
    }


@router.get("/{property_id}/similar", response_model=List[PropertySearchResult])
@cache_response(ttl=300)
async def get_similar_properties(
    property_id: int,
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get similar properties based on location and features"""
    
    # Get reference property
    ref_query = text("""
        SELECT 
            property_type,
            location_city,
            bedrooms,
            base_price,
            longitude,
            latitude
        FROM silver.properties
        WHERE property_id = :property_id
    """)
    
    ref_result = db.execute(ref_query, {'property_id': property_id})
    ref_property = ref_result.first()
    
    if not ref_property:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property {property_id} not found"
        )
    
    # Find similar properties
    similar_query = text("""
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
            COALESCE(array_length(p.amenities, 1), 0) as amenities_count,
            COALESCE(p.property_rating, 0) as rating,
            COALESCE(p.review_count, 0) as review_count,
            p.property_images[1] as primary_image,
            ST_Distance(
                p.location_point,
                ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326)::geography
            ) / 1000.0 as distance_km,
            -- Similarity score (simplified)
            (
                CASE WHEN p.property_type = :property_type THEN 0.3 ELSE 0 END +
                CASE WHEN p.bedrooms = :bedrooms THEN 0.3 ELSE 0 END +
                CASE WHEN ABS(p.base_price - :base_price) < :base_price * 0.2 THEN 0.2 ELSE 0 END +
                0.2  -- Location component (all in same city)
            ) as similarity_score
        FROM silver.properties p
        WHERE 
            p.is_active = TRUE
            AND p.property_id != :property_id
            AND p.location_city = :city
            AND ST_DWithin(
                p.location_point,
                ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326)::geography,
                5000  -- 5km radius
            )
        ORDER BY similarity_score DESC, distance_km ASC
        LIMIT :limit
    """)
    
    result = db.execute(similar_query, {
        'property_id': property_id,
        'property_type': ref_property.property_type,
        'city': ref_property.location_city,
        'bedrooms': ref_property.bedrooms,
        'base_price': ref_property.base_price,
        'longitude': ref_property.longitude,
        'latitude': ref_property.latitude,
        'limit': limit
    })
    
    return [dict(row._mapping) for row in result]


@router.get("/{property_id}/reviews")
@cache_response(ttl=120)
async def get_property_reviews(
    property_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """Get property reviews"""
    
    paginator = Paginator(page=page, page_size=page_size)
    
    query = text("""
        SELECT 
            review_id,
            guest_id,
            rating,
            review_text,
            sentiment_score,
            sentiment_label,
            created_at
        FROM silver.reviews
        WHERE property_id = :property_id
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """)
    
    count_query = text("""
        SELECT COUNT(*) FROM silver.reviews
        WHERE property_id = :property_id
    """)
    
    result = db.execute(query, {
        'property_id': property_id,
        'limit': paginator.limit,
        'offset': paginator.offset
    })
    
    count_result = db.execute(count_query, {'property_id': property_id})
    total_count = count_result.scalar()
    
    reviews = [
        {
            'review_id': row.review_id,
            'guest_id': row.guest_id,
            'rating': row.rating,
            'review_text': row.review_text,
            'sentiment_score': float(row.sentiment_score) if row.sentiment_score else None,
            'sentiment_label': row.sentiment_label,
            'created_at': row.created_at.isoformat()
        }
        for row in result
    ]
    
    return {
        'data': reviews,
        'pagination': paginator.get_metadata(total_count)
    }