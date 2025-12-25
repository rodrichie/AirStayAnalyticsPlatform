"""
Booking Endpoints
Create, retrieve, and manage bookings
"""
import logging
from typing import List
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text

from api.dependencies import get_db, get_redis
from api.schemas import BookingCreate, BookingResponse, SuccessResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bookings", tags=["Bookings"])


@router.post("", response_model=BookingResponse, status_code=status.HTTP_201_CREATED)
async def create_booking(
    booking: BookingCreate,
    db: Session = Depends(get_db),
    redis=Depends(get_redis)
):
    """
    Create a new booking
    
    Validates:
    - Property availability
    - Guest capacity
    - Minimum nights requirement
    """
    logger.info(f"Creating booking for property {booking.property_id}")
    
    # Check property exists and is active
    property_query = text("""
        SELECT 
            property_id,
            base_price,
            cleaning_fee,
            max_guests,
            minimum_nights
        FROM silver.properties
        WHERE property_id = :property_id AND is_active = TRUE
    """)
    
    property_result = db.execute(
        property_query,
        {'property_id': booking.property_id}
    )
    property_data = property_result.first()
    
    if not property_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Property not found or inactive"
        )
    
    # Validate guest capacity
    if booking.num_guests > property_data.max_guests:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Guest capacity exceeded (max: {property_data.max_guests})"
        )
    
    # Calculate nights
    nights = (booking.check_out_date - booking.check_in_date).days
    
    # Validate minimum nights
    if nights < property_data.minimum_nights:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Minimum {property_data.minimum_nights} nights required"
        )
    
    # Check availability
    availability_query = text("""
        SELECT COUNT(*) as unavailable_count
        FROM silver.availability_calendar
        WHERE 
            property_id = :property_id
            AND calendar_date >= :check_in
            AND calendar_date < :check_out
            AND is_available = FALSE
    """)
    
    avail_result = db.execute(availability_query, {
        'property_id': booking.property_id,
        'check_in': booking.check_in_date,
        'check_out': booking.check_out_date
    })
    
    if avail_result.scalar() > 0:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Property not available for selected dates"
        )
    
    # Calculate pricing
    base_price = float(property_data.base_price) * nights
    cleaning_fee = float(property_data.cleaning_fee or 0)
    service_fee = base_price * 0.12  # 12% service fee
    total_price = base_price + cleaning_fee + service_fee
    
    # Insert booking
    insert_query = text("""
        INSERT INTO silver.bookings (
            property_id, guest_id, check_in_date, check_out_date,
            nights, num_guests, base_price, cleaning_fee, service_fee,
            total_price, booking_status, booking_channel, special_requests,
            created_at, confirmed_at
        ) VALUES (
            :property_id, :guest_id, :check_in_date, :check_out_date,
            :nights, :num_guests, :base_price, :cleaning_fee, :service_fee,
            :total_price, 'confirmed', 'api', :special_requests,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
        RETURNING booking_id
    """)
    
    result = db.execute(insert_query, {
        'property_id': booking.property_id,
        'guest_id': booking.guest_id,
        'check_in_date': booking.check_in_date,
        'check_out_date': booking.check_out_date,
        'nights': nights,
        'num_guests': booking.num_guests,
        'base_price': base_price,
        'cleaning_fee': cleaning_fee,
        'service_fee': service_fee,
        'total_price': total_price,
        'special_requests': booking.special_requests
    })
    
    booking_id = result.scalar()
    
    # Update availability calendar
    update_avail_query = text("""
        UPDATE silver.availability_calendar
        SET is_available = FALSE, updated_at = CURRENT_TIMESTAMP
        WHERE 
            property_id = :property_id
            AND calendar_date >= :check_in
            AND calendar_date < :check_out
    """)
    
    db.execute(update_avail_query, {
        'property_id': booking.property_id,
        'check_in': booking.check_in_date,
        'check_out': booking.check_out_date
    })
    
    db.commit()
    
    # Invalidate cache
    try:
        redis.delete(f"availability:{booking.property_id}:*")
    except Exception as e:
        logger.warning(f"Cache invalidation failed: {e}")
    
    # Publish booking event to Kafka (if available)
    # from streaming.producers.event_producer import AirStayEventProducer
    # producer = AirStayEventProducer()
    # producer.produce_booking_event(...)
    
    return {
        'booking_id': booking_id,
        'property_id': booking.property_id,
        'guest_id': booking.guest_id,
        'check_in_date': booking.check_in_date,
        'check_out_date': booking.check_out_date,
        'nights': nights,
        'num_guests': booking.num_guests,
        'base_price': base_price,
        'cleaning_fee': cleaning_fee,
        'service_fee': service_fee,
        'total_price': total_price,
        'booking_status': 'confirmed',
        'created_at': datetime.now()
    }


@router.get("/{booking_id}", response_model=BookingResponse)
async def get_booking(
    booking_id: int,
    db: Session = Depends(get_db)
):
    """Get booking details"""
    
    query = text("""
        SELECT 
            booking_id, property_id, guest_id,
            check_in_date, check_out_date, nights, num_guests,
            base_price, cleaning_fee, service_fee, total_price,
            booking_status, created_at
        FROM silver.bookings
        WHERE booking_id = :booking_id
    """)
    
    result = db.execute(query, {'booking_id': booking_id})
    booking_data = result.first()
    
    if not booking_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Booking {booking_id} not found"
        )
    
    return dict(booking_data._mapping)


@router.post("/{booking_id}/cancel", response_model=SuccessResponse)
async def cancel_booking(
    booking_id: int,
    db: Session = Depends(get_db),
    redis=Depends(get_redis)
):
    """Cancel a booking"""
    
    # Get booking
    get_query = text("""
        SELECT property_id, check_in_date, check_out_date, booking_status
        FROM silver.bookings
        WHERE booking_id = :booking_id
    """)
    
    result = db.execute(get_query, {'booking_id': booking_id})
    booking = result.first()
    
    if not booking:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Booking {booking_id} not found"
        )
    
    if booking.booking_status == 'canceled':
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Booking already canceled"
        )
    
    # Update booking status
    cancel_query = text("""
        UPDATE silver.bookings
        SET 
            booking_status = 'canceled',
            canceled_at = CURRENT_TIMESTAMP,
            cancellation_reason = 'User requested'
        WHERE booking_id = :booking_id
    """)
    
    db.execute(cancel_query, {'booking_id': booking_id})
    
    # Release availability
    release_query = text("""
        UPDATE silver.availability_calendar
        SET is_available = TRUE, updated_at = CURRENT_TIMESTAMP
        WHERE 
            property_id = :property_id
            AND calendar_date >= :check_in
            AND calendar_date < :check_out
    """)
    
    db.execute(release_query, {
        'property_id': booking.property_id,
        'check_in': booking.check_in_date,
        'check_out': booking.check_out_date
    })
    
    db.commit()
    
    # Invalidate cache
    try:
        redis.delete(f"availability:{booking.property_id}:*")
    except Exception as e:
        logger.warning(f"Cache invalidation failed: {e}")
    
    return {
        'success': True,
        'message': f'Booking {booking_id} canceled successfully'
    }