"""
Pydantic Schemas for API Request/Response
"""
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator


# Property Schemas
class PropertyBase(BaseModel):
    """Base property schema"""
    listing_id: str
    title: str
    property_type: str
    city: str
    neighborhood: Optional[str] = None
    bedrooms: int
    bathrooms: float
    max_guests: int
    base_price: float
    amenities_count: int
    rating: Optional[float] = None
    review_count: int


class PropertyDetail(PropertyBase):
    """Detailed property schema"""
    property_id: int
    description: Optional[str] = None
    latitude: float
    longitude: float
    amenities: List[str] = []
    instant_bookable: bool
    minimum_nights: int
    cancellation_policy: str
    host_is_superhost: bool
    quality_score: float
    property_images: List[str] = []
    
    class Config:
        from_attributes = True


class PropertySearch(BaseModel):
    """Property search parameters"""
    city: Optional[str] = None
    check_in: Optional[date] = None
    check_out: Optional[date] = None
    num_guests: Optional[int] = None
    min_bedrooms: Optional[int] = None
    max_price: Optional[float] = None
    property_type: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    radius_km: Optional[float] = 5.0


class PropertySearchResult(PropertyBase):
    """Property search result with distance"""
    property_id: int
    distance_km: Optional[float] = None
    availability_status: str = "available"
    total_price: Optional[float] = None
    primary_image: Optional[str] = None


# Booking Schemas
class BookingCreate(BaseModel):
    """Create booking request"""
    property_id: int
    guest_id: int
    check_in_date: date
    check_out_date: date
    num_guests: int
    special_requests: Optional[str] = None
    
    @validator('check_out_date')
    def check_out_after_check_in(cls, v, values):
        if 'check_in_date' in values and v <= values['check_in_date']:
            raise ValueError('check_out_date must be after check_in_date')
        return v


class BookingResponse(BaseModel):
    """Booking response"""
    booking_id: int
    property_id: int
    guest_id: int
    check_in_date: date
    check_out_date: date
    nights: int
    num_guests: int
    base_price: float
    cleaning_fee: float
    service_fee: float
    total_price: float
    booking_status: str
    created_at: datetime
    
    class Config:
        from_attributes = True


# Pricing Schemas
class PriceRecommendation(BaseModel):
    """Price recommendation response"""
    property_id: int
    current_price: float
    recommended_price: float
    price_change: float
    price_change_pct: float
    confidence_interval_lower: float
    confidence_interval_upper: float
    recommendation_reason: str = "ML model prediction"


# Recommendation Schemas
class RecommendationRequest(BaseModel):
    """Recommendation request"""
    user_id: int
    n_recommendations: int = Field(10, ge=1, le=50)
    city: Optional[str] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None


class RecommendationResponse(BaseModel):
    """Recommendation response"""
    property_id: int
    listing_id: str
    title: str
    property_type: str
    city: str
    bedrooms: int
    base_price: float
    rating: float
    review_count: int
    image_url: Optional[str] = None
    recommendation_score: float
    recommendation_reason: str


# Analytics Schemas
class PropertyPerformance(BaseModel):
    """Property performance metrics"""
    property_id: int
    metric_date: date
    bookings_count: int
    revenue_total: float
    nights_booked: int
    occupancy_rate: float
    avg_nightly_rate: float
    avg_rating: float


class CityMetrics(BaseModel):
    """City-level metrics"""
    city: str
    metric_date: date
    active_properties: int
    total_bookings: int
    total_revenue: float
    avg_occupancy_rate: float
    avg_nightly_rate: float


# Anomaly Schemas
class AnomalyDetection(BaseModel):
    """Anomaly detection result"""
    entity_type: str
    entity_id: int
    is_anomaly: bool
    anomaly_score: float
    risk_level: str
    flags: List[str]
    recommendation: str


# Response Wrappers
class PaginatedResponse(BaseModel):
    """Paginated response wrapper"""
    data: List[Any]
    pagination: Dict[str, Any]


class SuccessResponse(BaseModel):
    """Generic success response"""
    success: bool = True
    message: str
    data: Optional[Any] = None


class ErrorResponse(BaseModel):
    """Error response"""
    success: bool = False
    error: str
    detail: Optional[str] = None