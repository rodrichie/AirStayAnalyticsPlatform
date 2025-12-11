"""
Event Schema Definitions
Defines JSON schemas for all event types in the system
"""
from datetime import datetime
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, validator
from enum import Enum

class EventType(str, Enum):
    "Event type enumeration"
    BOOKING_CREATED = "booking.created"
    BOOKING_CONFIRMED = "booking.confirmed"
    BOOKING_CANCELED = "booking.canceled"
    BOOKING_COMPLETED = "booking.completed"
    REVIEW_SUBMITTED = "review.submitted"
    REVIEW_UPDATED = "review.updated"
    SEARCH_PERFORMED = "search.performed"
    PROPERTY_VIEWED = "property.viewed"
    AVAILABILITY_UPDATED = "availability.updated"
    PRICE_CHANGED = "price.changed"
    USER_LOGGED_IN = "user.logged_in"

class BookingEVentPayload(BaseModel):
    """Booking event payload"""
    booking_id: int
    property_id: int
    guest_id: int
    host_id: int
    check_in_date: str
    check_out_date: str
    nights: int
    num_guests: int
    base_price: float
    cleaning_fee: float
    service_fee: float
    total_price: float
    booking_status: str
    booking_channel: str
    special_requests: Optional[str] = None
    
    @validator('check_in_date', 'check_out_date')
    def validate_date_format(cls, v):
        """Ensure dates are in ISO format"""
        try:
            datetime.fromisoformat(v)
        except ValueError:
            raise ValueError(f"Date must be in ISO format: {v}")
        return v

class BookingEvent(BaseModel):
    """Complete booking event"""
    event_id: str
    event_type: EventType
    event_timestamp: datetime
    property_id: int
    user_id: int
    payload: BookingEventPayload
    metadata: Dict = Field(default_factory=dict)

class ReviewEventPayload(BaseModel):
    """Review event payload"""
    review_id: int
    property_id: int
    booking_id: int
    guest_id: int
    rating: int = Field(..., ge=1, le=5)
    cleanliness_rating: Optional[int] = Field(None, ge=1, le=5)
    accuracy_rating: Optional[int] = Field(None, ge=1, le=5)
    communication_rating: Optional[int] = Field(None, ge=1, le=5)
    location_rating: Optional[int] = Field(None, ge=1, le=5)
    value_rating: Optional[int] = Field(None, ge=1, le=5)
    review_text: Optional[str] = None
    review_language: str = "en"   

class ReviewEvent(BaseModel):
    """Complete review event"""
    event_id: str
    event_type: EventType
    event_timestamp: datetime
    property_id: int
    user_id: int
    payload: ReviewEventPayload
    metadata: Dict = Field(default_factory=dict)

class SearchEventPayload(BaseModel):
    """Search event payload"""
    search_id: str
    city: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    check_in_date: Optional[str] = None
    check_out_date: Optional[str] = None
    num_guests: Optional[int] = None
    min_bedrooms: Optional[int] = None
    max_price: Optional[float] = None
    property_type: Optional[str] = None
    amenities: List[str] = Field(default_factory=list)
    results_count: int = 0
    search_duration_ms: int = 0      

class SearchEvent(BaseModel):
    """Complete search event"""
    event_id: str
    event_type: EventType
    event_timestamp: datetime
    user_id: Optional[int] = None
    session_id: str
    payload: SearchEventPayload
    metadata: Dict = Field(default_factory=dict)   

class PropertyViewEventPayload(BaseModel):
    """Property view event payload"""
    property_id: int
    view_duration_seconds: Optional[int] = None
    source: str  # search, recommendation, direct
    position_in_results: Optional[int] = None

class PropertyViewEvent(BaseModel):
    """Complete property view event"""
    event_id: str
    event_type: EventType
    event_timestamp: datetime
    property_id: int
    user_id: Optional[int] = None
    session_id: str
    payload: PropertyViewEventPayload
    metadata: Dict = Field(default_factory=dict)  

class AvailabilityUpdatePayload(BaseModel):
    """Availability update payload"""
    property_id: int
    date: str
    is_available: bool
    price: float
    minimum_nights: int = 1
    reason: str  # booking, cancellation, manual_update

class AvailabilityUpdateEvent(BaseModel):
    """Complete availability update event"""
    event_id: str
    event_type: EventType
    event_timestamp: datetime
    property_id: int
    payload: AvailabilityUpdatePayload
    metadata: Dict = Field(default_factory=dict)


class PriceChangePayload(BaseModel):
    """Price change payload"""
    property_id: int
    old_price: float
    new_price: float
    change_percentage: float
    reason: str  # dynamic_pricing, manual, seasonal
    effective_date: str


class PriceChangeEvent(BaseModel):
    """Complete price change event"""
    event_id: str
    event_type: EventType
    event_timestamp: datetime
    property_id: int
    payload: PriceChangePayload
    metadata: Dict = Field(default_factory=dict)


# Event type to schema mapping
EVENT_SCHEMAS = {
    EventType.BOOKING_CREATED: BookingEvent,
    EventType.BOOKING_CONFIRMED: BookingEvent,
    EventType.BOOKING_CANCELED: BookingEvent,
    EventType.BOOKING_COMPLETED: BookingEvent,
    EventType.REVIEW_SUBMITTED: ReviewEvent,
    EventType.REVIEW_UPDATED: ReviewEvent,
    EventType.SEARCH_PERFORMED: SearchEvent,
    EventType.PROPERTY_VIEWED: PropertyViewEvent,
    EventType.AVAILABILITY_UPDATED: AvailabilityUpdateEvent,
    EventType.PRICE_CHANGED: PriceChangeEvent,
}                             