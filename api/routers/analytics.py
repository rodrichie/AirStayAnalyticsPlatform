"""
Analytics Endpoints
Performance metrics and business intelligence
"""
import logging
from typing import List, Optional
from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from api.dependencies import get_db, cache_response
from api.schemas import PropertyPerformance, CityMetrics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/property/{property_id}/performance", response_model=List[PropertyPerformance])
@cache_response(ttl=3600)
async def get_property_performance(
    property_id: int,
    start_date: date = Query(...),
    end_date: date = Query(...),
    db: Session = Depends(get_db)
):
    """
    Get property performance metrics over time
    
    Metrics include:
    - Bookings count
    - Revenue
    - Occupancy rate
    - Average rating
    """
    query = text("""
        SELECT 
            property_id,
            metric_date,
            bookings_count,
            revenue_total,
            nights_booked,
            occupancy_rate,
            avg_nightly_rate,
            avg_rating
        FROM gold.agg_property_performance
        WHERE 
            property_id = :property_id
            AND metric_date >= :start_date
            AND metric_date <= :end_date
        ORDER BY metric_date
    """)
    
    result = db.execute(query, {
        'property_id': property_id,
        'start_date': start_date,
        'end_date': end_date
    })
    
    return [
        {
            'property_id': row.property_id,
            'metric_date': row.metric_date,
            'bookings_count': row.bookings_count,
            'revenue_total': float(row.revenue_total),
            'nights_booked': row.nights_booked,
            'occupancy_rate': float(row.occupancy_rate),
            'avg_nightly_rate': float(row.avg_nightly_rate),
            'avg_rating': float(row.avg_rating) if row.avg_rating else 0
        }
        for row in result
    ]


@router.get("/city/{city}/metrics", response_model=List[CityMetrics])
@cache_response(ttl=3600)
async def get_city_metrics(
    city: str,
    start_date: date = Query(...),
    end_date: date = Query(...),
    db: Session = Depends(get_db)
):
    """Get city-level performance metrics"""
    
    query = text("""
        SELECT 
            city,
            metric_date,
            active_properties,
            total_bookings,
            total_revenue,
            avg_occupancy_rate,
            avg_nightly_rate
        FROM gold.agg_city_metrics
        WHERE 
            city = :city
            AND metric_date >= :start_date
            AND metric_date <= :end_date
        ORDER BY metric_date
    """)
    
    result = db.execute(query, {
        'city': city,
        'start_date': start_date,
        'end_date': end_date
    })
    
    return [dict(row._mapping) for row in result]


@router.get("/dashboard/summary")
@cache_response(ttl=300)  # 5 minute cache
async def get_dashboard_summary(
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db)
):
    """
    Get high-level dashboard summary
    
    Returns:
    - Total bookings
    - Total revenue
    - Active properties
    - Average metrics
    """
    query = text("""
        WITH date_range AS (
            SELECT 
                CURRENT_DATE - INTERVAL '%s days' as start_date,
                CURRENT_DATE as end_date
        )
        SELECT 
            -- Booking metrics
            COUNT(DISTINCT b.booking_id) as total_bookings,
            SUM(b.total_price) as total_revenue,
            AVG(b.total_price) as avg_booking_value,
            COUNT(DISTINCT b.guest_id) as unique_guests,
            
            -- Property metrics
            COUNT(DISTINCT b.property_id) as properties_with_bookings,
            AVG(b.nights) as avg_nights_per_booking,
            
            -- Growth metrics (compare to previous period)
            COUNT(DISTINCT CASE 
                WHEN b.created_at >= dr.end_date - INTERVAL '%s days'
                THEN b.booking_id 
            END) as bookings_current_period,
            
            COUNT(DISTINCT CASE 
                WHEN b.created_at >= dr.end_date - INTERVAL '%s days' * 2
                AND b.created_at < dr.end_date - INTERVAL '%s days'
                THEN b.booking_id 
            END) as bookings_previous_period
            
        FROM silver.bookings b
        CROSS JOIN date_range dr
        WHERE 
            b.created_at >= dr.start_date
            AND b.booking_status IN ('confirmed', 'completed')
    """ % (days, days, days, days))
    
    result = db.execute(query)
    summary = result.first()
    
    # Calculate growth
    growth_rate = 0
    if summary.bookings_previous_period > 0:
        growth_rate = (
            (summary.bookings_current_period - summary.bookings_previous_period) /
            summary.bookings_previous_period * 100
        )
    
    # Get top cities
    top_cities_query = text("""
        SELECT 
            p.location_city as city,
            COUNT(b.booking_id) as bookings,
            SUM(b.total_price) as revenue
        FROM silver.bookings b
        JOIN silver.properties p ON b.property_id = p.property_id
        WHERE 
            b.created_at >= CURRENT_DATE - INTERVAL '%s days'
            AND b.booking_status IN ('confirmed', 'completed')
        GROUP BY p.location_city
        ORDER BY bookings DESC
        LIMIT 5
    """ % days)
    
    top_cities_result = db.execute(top_cities_query)
    
    return {
        'summary': {
            'total_bookings': int(summary.total_bookings),
            'total_revenue': float(summary.total_revenue),
            'avg_booking_value': float(summary.avg_booking_value),
            'unique_guests': int(summary.unique_guests),
            'active_properties': int(summary.properties_with_bookings),
            'avg_nights': float(summary.avg_nights_per_booking),
            'growth_rate': round(growth_rate, 2)
        },
        'top_cities': [
            {
                'city': row.city,
                'bookings': int(row.bookings),
                'revenue': float(row.revenue)
            }
            for row in top_cities_result
        ],
        'period_days': days
    }


@router.get("/metrics/real-time")
@cache_response(ttl=60)  # 1 minute cache
async def get_real_time_metrics(
    db: Session = Depends(get_db)
):
    """
    Get real-time platform metrics
    
    Returns last hour activity
    """
    query = text("""
        SELECT 
            COUNT(*) FILTER (
                WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
            ) as bookings_last_hour,
            
            COUNT(*) FILTER (
                WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
                AND booking_status = 'confirmed'
            ) as confirmed_last_hour,
            
            SUM(total_price) FILTER (
                WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
            ) as revenue_last_hour,
            
            COUNT(DISTINCT guest_id) FILTER (
                WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
            ) as active_users_last_hour
            
        FROM silver.bookings
    """)
    
    result = db.execute(query)
    metrics = result.first()
    
    return {
        'bookings_last_hour': int(metrics.bookings_last_hour or 0),
        'confirmed_last_hour': int(metrics.confirmed_last_hour or 0),
        'revenue_last_hour': float(metrics.revenue_last_hour or 0),
        'active_users_last_hour': int(metrics.active_users_last_hour or 0),
        'timestamp': date.today().isoformat()
    }