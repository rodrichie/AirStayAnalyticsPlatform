"""
Event Simulator
Generates realistic event streams for testing and development
"""
import random
import time
import logging
from datetime import datetime, timedelta
from typing import List
import uuid

from streaming.producers.event_producer import AirStayEventProducer
from streaming.schemas.event_schemas import EventType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EventSimulator:
    """Simulate realistic event streams"""
    
    def __init__(self, producer: AirStayEventProducer):
        """Initialize simulator"""
        self.producer = producer
        
        # Sample data
        self.property_ids = list(range(1, 501))  # 500 properties
        self.user_ids = list(range(1001, 2001))  # 1000 users
        self.cities = [
            'New York', 'Los Angeles', 'San Francisco', 'Chicago',
            'Miami', 'Seattle', 'Austin', 'Boston'
        ]
        self.property_types = [
            'Entire apartment', 'Entire house', 'Private room',
            'Entire villa', 'Entire condo'
        ]
    
    def generate_search_events(self, count: int = 10, delay_ms: int = 100):
        """Generate search events"""
        logger.info(f"🔍 Generating {count} search events...")
        
        for i in range(count):
            search_data = {
                'search_id': str(uuid.uuid4()),
                'city': random.choice(self.cities),
                'check_in_date': (datetime.now() + timedelta(days=random.randint(7, 60))).strftime('%Y-%m-%d'),
                'check_out_date': (datetime.now() + timedelta(days=random.randint(61, 90))).strftime('%Y-%m-%d'),
                'num_guests': random.randint(1, 6),
                'min_bedrooms': random.choice([None, 1, 2, 3]),
                'max_price': random.choice([None, 150, 250, 500]),
                'property_type': random.choice([None] + self.property_types),
                'results_count': random.randint(5, 100),
                'search_duration_ms': random.randint(50, 500)
            }
            
            self.producer.produce_search_event(
                search_data=search_data,
                user_id=random.choice(self.user_ids),
                session_id=f"session-{uuid.uuid4()}"
            )
            
            time.sleep(delay_ms / 1000.0)
        
        logger.info(f"✅ Generated {count} search events")
    
    def generate_booking_events(self, count: int = 5, delay_ms: int = 500):
        """Generate booking events"""
        logger.info(f"📅 Generating {count} booking events...")
        
        for i in range(count):
            property_id = random.choice(self.property_ids)
            guest_id = random.choice(self.user_ids)
            check_in = datetime.now() + timedelta(days=random.randint(7, 60))
            nights = random.randint(2, 7)
            check_out = check_in + timedelta(days=nights)
            base_price = random.uniform(80, 400)
            
            booking_data = {
                'booking_id': random.randint(10000, 99999),
                'property_id': property_id,
                'guest_id': guest_id,
                'host_id': random.randint(1, 100),
                'check_in_date': check_in.strftime('%Y-%m-%d'),
                'check_out_date': check_out.strftime('%Y-%m-%d'),
                'nights': nights,
                'num_guests': random.randint(1, 4),
                'base_price': base_price,
                'cleaning_fee': base_price * 0.15,
                'service_fee': base_price * 0.12,
                'total_price': base_price * 1.27,
                'booking_status': 'confirmed',
                'booking_channel': random.choice(['web', 'mobile', 'api'])
            }
            
            # Booking created
            self.producer.produce_booking_event(
                event_type=EventType.BOOKING_CREATED,
                booking_data=booking_data
            )
            
            time.sleep(delay_ms / 1000.0)
            
            # Booking confirmed (80% of bookings)
            if random.random() < 0.8:
                booking_data['booking_status'] = 'confirmed'
                self.producer.produce_booking_event(
                    event_type=EventType.BOOKING_CONFIRMED,
                    booking_data=booking_data
                )
                
                # Also produce availability update
                for day in range(nights):
                    date = (check_in + timedelta(days=day)).strftime('%Y-%m-%d')
                    self.producer.produce_availability_update(
                        property_id=property_id,
                        date=date,
                        is_available=False,
                        price=base_price,
                        reason='booking'
                    )
        
        logger.info(f"✅ Generated {count} booking events")
    
    def generate_review_events(self, count: int = 5, delay_ms: int = 300):
        """Generate review events"""
        logger.info(f"⭐ Generating {count} review events...")
        
        for i in range(count):
            rating = random.randint(3, 5)
            
            review_data = {
                'review_id': random.randint(1000, 9999),
                'property_id': random.choice(self.property_ids),
                'booking_id': random.randint(10000, 99999),
                'guest_id': random.choice(self.user_ids),
                'rating': rating,
                'cleanliness_rating': rating + random.randint(-1, 1),
                'accuracy_rating': rating + random.randint(-1, 1),
                'communication_rating': rating + random.randint(-1, 1),
                'location_rating': rating + random.randint(-1, 1),
                'value_rating': rating + random.randint(-1, 1),
                'review_text': self._generate_review_text(rating),
                'review_language': random.choice(['en', 'es', 'fr', 'de'])
            }
            
            self.producer.produce_review_event(
                event_type=EventType.REVIEW_SUBMITTED,
                review_data=review_data
            )
            
            time.sleep(delay_ms / 1000.0)
        
        logger.info(f"✅ Generated {count} review events")
    
    def _generate_review_text(self, rating: int) -> str:
        """Generate sample review text based on rating"""
        positive_reviews = [
            "Great place! Very clean and comfortable.",
            "Amazing location, perfect for exploring the city.",
            "Host was very responsive and helpful.",
            "Exceeded our expectations, highly recommend!",
            "Beautiful property with all the amenities we needed."
        ]
        
        neutral_reviews = [
            "Decent place, met our basic needs.",
            "Good location but could use some updates.",
            "Average experience overall."
        ]
        
        negative_reviews = [
            "Property wasn't as described in photos.",
            "Had some cleanliness issues.",
            "Location was not ideal for our needs."
        ]
        
        if rating >= 4:
            return random.choice(positive_reviews)
        elif rating == 3:
            return random.choice(neutral_reviews)
        else:
            return random.choice(negative_reviews)
    
    def generate_continuous_stream(
        self,
        duration_seconds: int = 60,
        events_per_second: int = 5
    ):
        """Generate continuous event stream"""
        logger.info(
            f"🌊 Starting continuous event stream: "
            f"{events_per_second} events/sec for {duration_seconds}s"
        )
        
        start_time = time.time()
        event_count = 0
        
        while time.time() - start_time < duration_seconds:
            # Weighted random event generation
            event_type = random.choices(
                ['search', 'booking', 'review', 'view'],
                weights=[0.5, 0.2, 0.1, 0.2],
                k=1
            )[0]
            
            if event_type == 'search':
                self.generate_search_events(count=1, delay_ms=0)
            elif event_type == 'booking':
                self.generate_booking_events(count=1, delay_ms=0)
            elif event_type == 'review':
                self.generate_review_events(count=1, delay_ms=0)
            
            event_count += 1
            
            # Sleep to maintain events per second rate
            time.sleep(1.0 / events_per_second)
        
        logger.info(f"✅ Generated {event_count} events in {duration_seconds}s")


# Example usage
if __name__ == "__main__":
    producer = AirStayEventProducer()
    simulator = EventSimulator(producer)
    
    # Generate sample events
    simulator.generate_search_events(count=20)
    simulator.generate_booking_events(count=10)
    simulator.generate_review_events(count=5)
    
    # Or run continuous stream
    # simulator.generate_continuous_stream(duration_seconds=300, events_per_second=10)
    
    producer.close()