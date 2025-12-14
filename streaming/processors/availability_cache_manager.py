"""
Real-Time Availability Cache Manager
Maintains Redis cache synchronized with availability updates from Kafka
"""
import json
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Set, Optional
import redis
from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AvailabilityCacheManager:
    """Manage real-time availability cache with Redis"""
    
    def __init__(
        self,
        redis_url: str = "redis://redis:6379",
        kafka_bootstrap_servers: str = "kafka:9092",
        db_config: Dict = None
    ):
        """Initialize cache manager"""
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
        self.redis_pipeline = self.redis_client.pipeline()
        
        # Kafka consumer configuration
        self.consumer = Consumer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'availability-cache-updater',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'max.poll.interval.ms': 300000,
        })
        
        # Subscribe to availability updates
        self.consumer.subscribe(['availability-updates', 'booking-events'])
        
        # Database config
        self.db_config = db_config or {
            'host': 'postgres',
            'database': 'airstay_db',
            'user': 'airstay',
            'password': 'airstay_pass'
        }
        
        logger.info("✅ Availability Cache Manager initialized")
    
    def get_cache_key(self, property_id: int, date: str) -> str:
        """Generate cache key for property-date combination"""
        return f"availability:{property_id}:{date}"
    
    def get_property_calendar_key(self, property_id: int) -> str:
        """Generate key for property's full calendar"""
        return f"calendar:{property_id}"
    
    def update_availability_cache(
        self,
        property_id: int,
        date: str,
        is_available: bool,
        price: float,
        minimum_nights: int = 1
    ):
        """
        Update availability cache for a specific date
        
        Args:
            property_id: Property ID
            date: Date string (YYYY-MM-DD)
            is_available: Availability status
            price: Nightly price
            minimum_nights: Minimum stay requirement
        """
        cache_key = self.get_cache_key(property_id, date)
        calendar_key = self.get_property_calendar_key(property_id)
        
        # Store availability data
        availability_data = {
            'property_id': property_id,
            'date': date,
            'is_available': is_available,
            'price': price,
            'minimum_nights': minimum_nights,
            'updated_at': datetime.utcnow().isoformat()
        }
        
        # Set with 7-day TTL (longer for future dates)
        ttl = 604800  # 7 days in seconds
        
        self.redis_client.setex(
            cache_key,
            ttl,
            json.dumps(availability_data)
        )
        
        # Also update sorted set for range queries
        # Score is timestamp for easy cleanup
        timestamp = datetime.fromisoformat(date).timestamp()
        self.redis_client.zadd(
            calendar_key,
            {date: timestamp}
        )
        
        logger.debug(f"✅ Updated cache for property {property_id}, date {date}")
    
    def bulk_update_availability(
        self,
        property_id: int,
        date_range: List[str],
        is_available: bool,
        price: float
    ):
        """Bulk update availability for multiple dates"""
        pipeline = self.redis_client.pipeline()
        calendar_key = self.get_property_calendar_key(property_id)
        
        for date_str in date_range:
            cache_key = self.get_cache_key(property_id, date_str)
            
            availability_data = {
                'property_id': property_id,
                'date': date_str,
                'is_available': is_available,
                'price': price,
                'updated_at': datetime.utcnow().isoformat()
            }
            
            pipeline.setex(cache_key, 604800, json.dumps(availability_data))
            
            timestamp = datetime.fromisoformat(date_str).timestamp()
            pipeline.zadd(calendar_key, {date_str: timestamp})
        
        pipeline.execute()
        
        logger.info(f"✅ Bulk updated {len(date_range)} dates for property {property_id}")
    
    def check_availability_cached(
        self,
        property_id: int,
        check_in: str,
        check_out: str
    ) -> Dict:
        """
        Check availability from cache
        
        Returns:
            Dictionary with availability status and details
        """
        # Generate all dates in range
        check_in_date = datetime.fromisoformat(check_in).date()
        check_out_date = datetime.fromisoformat(check_out).date()
        
        current_date = check_in_date
        all_available = True
        total_price = 0.0
        unavailable_dates = []
        
        while current_date < check_out_date:
            cache_key = self.get_cache_key(property_id, current_date.isoformat())
            cached_data = self.redis_client.get(cache_key)
            
            if cached_data:
                availability = json.loads(cached_data)
                
                if not availability['is_available']:
                    all_available = False
                    unavailable_dates.append(current_date.isoformat())
                else:
                    total_price += availability['price']
            else:
                # Cache miss - need to query database
                all_available = False
                unavailable_dates.append(current_date.isoformat())
            
            current_date += timedelta(days=1)
        
        nights = (check_out_date - check_in_date).days
        
        return {
            'property_id': property_id,
            'check_in': check_in,
            'check_out': check_out,
            'nights': nights,
            'is_available': all_available,
            'total_price': total_price if all_available else None,
            'avg_nightly_rate': total_price / nights if all_available and nights > 0 else None,
            'unavailable_dates': unavailable_dates,
            'cache_hit': len(unavailable_dates) == 0
        }
    
    def get_available_dates_range(
        self,
        property_id: int,
        start_date: str,
        end_date: str
    ) -> List[str]:
        """Get list of available dates in range"""
        calendar_key = self.get_property_calendar_key(property_id)
        
        start_ts = datetime.fromisoformat(start_date).timestamp()
        end_ts = datetime.fromisoformat(end_date).timestamp()
        
        # Get dates from sorted set
        dates = self.redis_client.zrangebyscore(
            calendar_key,
            start_ts,
            end_ts
        )
        
        available_dates = []
        
        for date_str in dates:
            cache_key = self.get_cache_key(property_id, date_str)
            cached_data = self.redis_client.get(cache_key)
            
            if cached_data:
                availability = json.loads(cached_data)
                if availability['is_available']:
                    available_dates.append(date_str)
        
        return available_dates
    
    def process_booking_event(self, event: Dict):
        """Process booking event and update availability"""
        event_type = event.get('event_type')
        payload = event.get('payload', {})
        
        property_id = payload.get('property_id')
        check_in = payload.get('check_in_date')
        check_out = payload.get('check_out_date')
        
        if not all([property_id, check_in, check_out]):
            logger.warning("Incomplete booking event, skipping")
            return
        
        # Generate date range
        check_in_date = datetime.fromisoformat(check_in).date()
        check_out_date = datetime.fromisoformat(check_out).date()
        
        date_range = []
        current_date = check_in_date
        
        while current_date < check_out_date:
            date_range.append(current_date.isoformat())
            current_date += timedelta(days=1)
        
        # Determine availability based on event type
        if event_type in ['booking.confirmed', 'booking.completed']:
            is_available = False
            reason = 'booking_confirmed'
        elif event_type == 'booking.canceled':
            is_available = True
            reason = 'booking_canceled'
        else:
            return
        
        # Get price from database (simplified - in production, might be in event)
        price = payload.get('base_price', 0.0) / payload.get('nights', 1)
        
        # Update cache
        self.bulk_update_availability(
            property_id=property_id,
            date_range=date_range,
            is_available=is_available,
            price=price
        )
        
        logger.info(
            f"✅ Updated availability for property {property_id}: "
            f"{len(date_range)} dates, available={is_available}"
        )
    
    def process_availability_event(self, event: Dict):
        """Process availability update event"""
        payload = event.get('payload', {})
        
        self.update_availability_cache(
            property_id=payload['property_id'],
            date=payload['date'],
            is_available=payload['is_available'],
            price=payload['price'],
            minimum_nights=payload.get('minimum_nights', 1)
        )
    
    def sync_from_database(self, property_id: int, days_ahead: int = 365):
        """Sync availability from database to cache"""
        logger.info(f"🔄 Syncing property {property_id} from database...")
        
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get availability for next N days
        query = """
            SELECT 
                calendar_date,
                is_available,
                price,
                minimum_nights
            FROM silver.availability_calendar
            WHERE 
                property_id = %s
                AND calendar_date >= CURRENT_DATE
                AND calendar_date <= CURRENT_DATE + INTERVAL '%s days'
            ORDER BY calendar_date
        """
        
        cursor.execute(query, (property_id, days_ahead))
        records = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Update cache
        for record in records:
            self.update_availability_cache(
                property_id=property_id,
                date=record['calendar_date'].isoformat(),
                is_available=record['is_available'],
                price=float(record['price']),
                minimum_nights=record['minimum_nights']
            )
        
        logger.info(f"✅ Synced {len(records)} records for property {property_id}")
        
        return len(records)
    
    def consume_events(self, timeout: int = 1.0):
        """Consume and process events from Kafka"""
        logger.info("👂 Starting event consumption...")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                # Parse event
                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    
                    # Route based on topic
                    if msg.topic() == 'booking-events':
                        self.process_booking_event(event)
                    elif msg.topic() == 'availability-updates':
                        self.process_availability_event(event)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse event: {e}")
                except Exception as e:
                    logger.error(f"Error processing event: {e}")
        
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            self.consumer.close()
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        info = self.redis_client.info('stats')
        memory = self.redis_client.info('memory')
        
        # Count availability keys
        availability_keys = 0
        for key in self.redis_client.scan_iter(match='availability:*'):
            availability_keys += 1
        
        return {
            'total_keys': self.redis_client.dbsize(),
            'availability_keys': availability_keys,
            'memory_used_mb': round(memory['used_memory'] / 1024 / 1024, 2),
            'total_commands_processed': info['total_commands_processed'],
            'instantaneous_ops_per_sec': info['instantaneous_ops_per_sec'],
            'keyspace_hits': info.get('keyspace_hits', 0),
            'keyspace_misses': info.get('keyspace_misses', 0),
        }


# Example usage
if __name__ == "__main__":
    manager = AvailabilityCacheManager()
    
    # Example 1: Sync property from database
    manager.sync_from_database(property_id=1, days_ahead=90)
    
    # Example 2: Check availability
    result = manager.check_availability_cached(
        property_id=1,
        check_in='2024-07-01',
        check_out='2024-07-05'
    )
    print(f"Availability check: {result}")
    
    # Example 3: Get cache stats
    stats = manager.get_cache_stats()
    print(f"Cache stats: {stats}")
    
    # Example 4: Start consuming events
    # manager.consume_events()