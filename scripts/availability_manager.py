"""
Availability Calendar Manager
Tracks property availability and manages booking conflicts
Uses Redis for real-time availability checks
"""
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import redis
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AvailabilityManager:
    """Manage property availability calendar"""
    
    def __init__(self, db_config: Dict = None, redis_url: str = None):
        """Initialize with database and Redis connections"""
        self.db_config = db_config or {
            'host': 'postgres',
            'database': 'airstay_db',
            'user': 'airstay',
            'password': 'airstay_pass'
        }
        
        redis_url = redis_url or 'redis://redis:6379'
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
        
        logger.info("✅ Availability Manager initialized")
    
    def _get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)
    
    def generate_availability_calendar(
        self,
        property_id: int,
        start_date: date,
        end_date: date,
        base_price: float,
        minimum_nights: int = 1
    ) -> int:
        """
        Generate availability calendar for a property
        
        Args:
            property_id: Property ID
            start_date: Calendar start date
            end_date: Calendar end date
            base_price: Default nightly price
            minimum_nights: Minimum stay requirement
            
        Returns:
            Number of days added
        """
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        # Generate list of dates
        current_date = start_date
        calendar_entries = []
        
        while current_date <= end_date:
            calendar_entries.append((
                property_id,
                current_date,
                True,  # is_available
                base_price,
                minimum_nights,
                365  # maximum_nights
            ))
            current_date += timedelta(days=1)
        
        # Bulk insert
        insert_query = """
            INSERT INTO silver.availability_calendar 
            (property_id, calendar_date, is_available, price, minimum_nights, maximum_nights)
            VALUES %s
            ON CONFLICT (property_id, calendar_date) 
            DO UPDATE SET
                price = EXCLUDED.price,
                minimum_nights = EXCLUDED.minimum_nights,
                updated_at = CURRENT_TIMESTAMP
        """
        
        execute_values(cursor, insert_query, calendar_entries)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Generated {len(calendar_entries)} days for property {property_id}")
        
        return len(calendar_entries)
    
    def check_availability(
        self,
        property_id: int,
        check_in: date,
        check_out: date
    ) -> bool:
        """
        Check if property is available for date range
        First checks Redis cache, falls back to database
        
        Args:
            property_id: Property ID
            check_in: Check-in date
            check_out: Check-out date
            
        Returns:
            True if available for entire range
        """
        # Try Redis cache first
        cache_key = f"availability:{property_id}:{check_in}:{check_out}"
        cached = self.redis_client.get(cache_key)
        
        if cached is not None:
            return cached == "1"
        
        # Query database
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT COUNT(*) as unavailable_days
            FROM silver.availability_calendar
            WHERE 
                property_id = %s
                AND calendar_date >= %s
                AND calendar_date < %s
                AND is_available = FALSE
        """
        
        cursor.execute(query, (property_id, check_in, check_out))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        is_available = result[0] == 0
        
        # Cache result for 5 minutes
        self.redis_client.setex(cache_key, 300, "1" if is_available else "0")
        
        return is_available
    
    def block_dates(
        self,
        property_id: int,
        check_in: date,
        check_out: date,
        booking_id: Optional[int] = None
    ) -> int:
        """
        Block dates for a booking
        
        Args:
            property_id: Property ID
            check_in: Check-in date
            check_out: Check-out date (exclusive)
            booking_id: Associated booking ID
            
        Returns:
            Number of nights blocked
        """
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        # Update availability
        update_query = """
            UPDATE silver.availability_calendar
            SET 
                is_available = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE 
                property_id = %s
                AND calendar_date >= %s
                AND calendar_date < %s
                AND is_available = TRUE
        """
        
        cursor.execute(update_query, (property_id, check_in, check_out))
        nights_blocked = cursor.rowcount
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Invalidate cache
        self._invalidate_availability_cache(property_id, check_in, check_out)
        
        logger.info(f"✅ Blocked {nights_blocked} nights for property {property_id}")
        
        return nights_blocked
    
    def unblock_dates(
        self,
        property_id: int,
        check_in: date,
        check_out: date
    ) -> int:
        """
        Unblock dates (e.g., after cancellation)
        """
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        update_query = """
            UPDATE silver.availability_calendar
            SET 
                is_available = TRUE,
                updated_at = CURRENT_TIMESTAMP
            WHERE 
                property_id = %s
                AND calendar_date >= %s
                AND calendar_date < %s
        """
        
        cursor.execute(update_query, (property_id, check_in, check_out))
        nights_unblocked = cursor.rowcount
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Invalidate cache
        self._invalidate_availability_cache(property_id, check_in, check_out)
        
        logger.info(f"✅ Unblocked {nights_unblocked} nights for property {property_id}")
        
        return nights_unblocked
    
    def get_available_properties(
        self,
        check_in: date,
        check_out: date,
        filters: Dict = None,
        limit: int = 50
    ) -> List[Dict]:
        """
        Get list of available properties for date range
        
        Args:
            check_in: Check-in date
            check_out: Check-out date
            filters: Additional filters (city, property_type, etc.)
            limit: Max results
        """
        conn = self._get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT DISTINCT
                p.property_id,
                p.listing_id,
                p.title,
                p.property_type,
                p.bedrooms,
                p.bathrooms,
                p.max_guests,
                p.location_city,
                p.latitude,
                p.longitude,
                AVG(ac.price) as avg_nightly_rate,
                p.property_rating,
                p.review_count,
                p.amenities
            FROM silver.properties p
            INNER JOIN silver.availability_calendar ac 
                ON p.property_id = ac.property_id
            WHERE 
                p.is_active = TRUE
                AND ac.calendar_date >= %s
                AND ac.calendar_date < %s
                AND ac.is_available = TRUE
            GROUP BY 
                p.property_id,
                p.listing_id,
                p.title,
                p.property_type,
                p.bedrooms,
                p.bathrooms,
                p.max_guests,
                p.location_city,
                p.latitude,
                p.longitude,
                p.property_rating,
                p.review_count,
                p.amenities
            HAVING 
                COUNT(*) = %s
        """
        
        nights = (check_out - check_in).days
        params = [check_in, check_out, nights]
        
        # Apply filters
        if filters:
            if filters.get('city'):
                query = query.replace(
                    "p.is_active = TRUE",
                    "p.is_active = TRUE AND p.location_city = %s"
                )
                params.insert(0, filters['city'])
            
            if filters.get('min_bedrooms'):
                query = query.replace(
                    "p.is_active = TRUE",
                    "p.is_active = TRUE AND p.bedrooms >= %s"
                )
                params.insert(0, filters['min_bedrooms'])
            
            if filters.get('max_guests'):
                query = query.replace(
                    "p.is_active = TRUE",
                    "p.is_active = TRUE AND p.max_guests >= %s"
                )
                params.insert(0, filters['max_guests'])
        
        query += " ORDER BY p.property_rating DESC NULLS LAST LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Found {len(results)} available properties")
        
        return [dict(row) for row in results]
    
    def calculate_occupancy_rate(
        self,
        property_id: int,
        start_date: date,
        end_date: date
    ) -> float:
        """
        Calculate occupancy rate for date range
        
        Returns:
            Occupancy rate as percentage (0-100)
        """
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                COUNT(*) as total_days,
                COUNT(*) FILTER (WHERE is_available = FALSE) as booked_days
            FROM silver.availability_calendar
            WHERE 
                property_id = %s
                AND calendar_date >= %s
                AND calendar_date <= %s
        """
        
        cursor.execute(query, (property_id, start_date, end_date))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result[0] == 0:
            return 0.0
        
        occupancy_rate = (result[1] / result[0]) * 100
        
        return round(occupancy_rate, 2)
    
    def _invalidate_availability_cache(
        self,
        property_id: int,
        start_date: date,
        end_date: date
    ):
        """Invalidate Redis cache for affected date ranges"""
        # Delete all cache keys for this property in the date range
        pattern = f"availability:{property_id}:*"
        
        for key in self.redis_client.scan_iter(match=pattern):
            self.redis_client.delete(key)
        
        logger.debug(f"Invalidated availability cache for property {property_id}")
    
    def sync_bookings_to_calendar(self) -> int:
        """
        Sync confirmed bookings to availability calendar
        Run this periodically to ensure consistency
        
        Returns:
            Number of bookings synced
        """
        conn = self._get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all confirmed bookings
        query = """
            SELECT 
                property_id,
                check_in_date,
                check_out_date,
                booking_id
            FROM silver.bookings
            WHERE booking_status = 'confirmed'
        """
        
        cursor.execute(query)
        bookings = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        synced_count = 0
        
        for booking in bookings:
            try:
                self.block_dates(
                    property_id=booking['property_id'],
                    check_in=booking['check_in_date'],
                    check_out=booking['check_out_date'],
                    booking_id=booking['booking_id']
                )
                synced_count += 1
            except Exception as e:
                logger.error(f"Error syncing booking {booking['booking_id']}: {e}")
        
        logger.info(f"✅ Synced {synced_count} bookings to calendar")
        
        return synced_count


# Example usage
if __name__ == "__main__":
    manager = AvailabilityManager()
    
    # Example 1: Generate calendar for next 365 days
    print("\n📅 Generating availability calendar...")
    days_added = manager.generate_availability_calendar(
        property_id=1,
        start_date=date.today(),
        end_date=date.today() + timedelta(days=365),
        base_price=150.00,
        minimum_nights=2
    )
    print(f"Added {days_added} days to calendar")
    
    # Example 2: Check availability
    print("\n🔍 Checking availability...")
    check_in = date.today() + timedelta(days=7)
    check_out = date.today() + timedelta(days=10)
    
    is_available = manager.check_availability(1, check_in, check_out)
    print(f"Property available {check_in} to {check_out}: {is_available}")
    
    # Example 3: Calculate occupancy
    print("\n📊 Calculating occupancy rate...")
    occupancy = manager.calculate_occupancy_rate(
        property_id=1,
        start_date=date.today(),
        end_date=date.today() + timedelta(days=30)
    )
    print(f"30-day occupancy rate: {occupancy}%")