"""
Kafka Event Producer
Publishes events to Kafka topics with retry logic and error handling
"""
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, Optional, Any
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import socket

from streaming.schemas.event_schemas import (
    EventType, EVENT_SCHEMAS,
    BookingEvent, ReviewEvent, SearchEvent
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AirStayEventProducer:
    """Kafka event producer for AirStay platform"""
    
    # Topic routing based on event type
    TOPIC_ROUTING = {
        EventType.BOOKING_CREATED: 'booking-events',
        EventType.BOOKING_CONFIRMED: 'booking-events',
        EventType.BOOKING_CANCELED: 'booking-events',
        EventType.BOOKING_COMPLETED: 'booking-events',
        EventType.REVIEW_SUBMITTED: 'review-events',
        EventType.REVIEW_UPDATED: 'review-events',
        EventType.SEARCH_PERFORMED: 'search-events',
        EventType.PROPERTY_VIEWED: 'user-activity',
        EventType.AVAILABILITY_UPDATED: 'availability-updates',
        EventType.PRICE_CHANGED: 'price-changes',
        EventType.USER_LOGGED_IN: 'user-activity',
    }
    
    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        """Initialize Kafka producer"""
        self.bootstrap_servers = bootstrap_servers
        
        # Producer configuration
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname(),
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
            'max.in.flight.requests.per.connection': 5,
            'compression.type': 'snappy',
            'linger.ms': 10,  # Batch messages for 10ms
            'batch.size': 32768,  # 32KB batch size
        }
        
        self.producer = Producer(conf)
        
        logger.info(f"✅ Kafka producer initialized: {bootstrap_servers}")
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err:
            logger.error(f"❌ Message delivery failed: {err}")
            logger.error(f"   Topic: {msg.topic()}, Partition: {msg.partition()}")
        else:
            logger.debug(
                f"✅ Message delivered to {msg.topic()} "
                f"[partition {msg.partition()}] at offset {msg.offset()}"
            )
    
    def produce_event(
        self,
        event_type: EventType,
        payload: Dict[str, Any],
        property_id: Optional[int] = None,
        user_id: Optional[int] = None,
        metadata: Optional[Dict] = None
    ) -> str:
        """
        Produce an event to the appropriate Kafka topic
        
        Args:
            event_type: Type of event
            payload: Event payload data
            property_id: Property ID (for routing)
            user_id: User ID
            metadata: Additional metadata
            
        Returns:
            Event ID
        """
        # Generate event ID
        event_id = str(uuid.uuid4())
        
        # Get appropriate schema
        event_schema = EVENT_SCHEMAS.get(event_type)
        
        if not event_schema:
            raise ValueError(f"Unknown event type: {event_type}")
        
        # Construct event
        event_data = {
            'event_id': event_id,
            'event_type': event_type.value,
            'event_timestamp': datetime.utcnow().isoformat(),
            'payload': payload,
            'metadata': metadata or {}
        }
        
        # Add contextual IDs
        if property_id:
            event_data['property_id'] = property_id
        if user_id:
            event_data['user_id'] = user_id
        
        # Validate against schema
        try:
            validated_event = event_schema(**event_data)
        except Exception as e:
            logger.error(f"Event validation failed: {e}")
            raise
        
        # Determine topic
        topic = self.TOPIC_ROUTING.get(event_type)
        if not topic:
            raise ValueError(f"No topic routing for event type: {event_type}")
        
        # Serialize to JSON
        message = json.dumps(validated_event.dict(), default=str)
        
        # Determine partition key (for ordering within property)
        partition_key = str(property_id) if property_id else str(user_id)
        
        try:
            # Produce message
            self.producer.produce(
                topic=topic,
                key=partition_key.encode('utf-8') if partition_key else None,
                value=message.encode('utf-8'),
                callback=self._delivery_callback
            )
            
            # Trigger delivery reports
            self.producer.poll(0)
            
            logger.info(
                f"📤 Event produced: {event_type.value} "
                f"(ID: {event_id[:8]}..., Topic: {topic})"
            )
            
            return event_id
            
        except Exception as e:
            logger.error(f"Failed to produce event: {e}")
            raise
    
    def produce_booking_event(
        self,
        event_type: EventType,
        booking_data: Dict[str, Any]
    ) -> str:
        """Produce a booking-related event"""
        return self.produce_event(
            event_type=event_type,
            payload=booking_data,
            property_id=booking_data.get('property_id'),
            user_id=booking_data.get('guest_id'),
            metadata={
                'source': 'booking_service',
                'version': '1.0'
            }
        )
    
    def produce_review_event(
        self,
        event_type: EventType,
        review_data: Dict[str, Any]
    ) -> str:
        """Produce a review-related event"""
        return self.produce_event(
            event_type=event_type,
            payload=review_data,
            property_id=review_data.get('property_id'),
            user_id=review_data.get('guest_id'),
            metadata={
                'source': 'review_service',
                'version': '1.0'
            }
        )
    
    def produce_search_event(
        self,
        search_data: Dict[str, Any],
        user_id: Optional[int] = None,
        session_id: str = None
    ) -> str:
        """Produce a search event"""
        event_data = {
            'event_id': str(uuid.uuid4()),
            'event_type': EventType.SEARCH_PERFORMED.value,
            'event_timestamp': datetime.utcnow().isoformat(),
            'user_id': user_id,
            'session_id': session_id or str(uuid.uuid4()),
            'payload': search_data,
            'metadata': {
                'source': 'search_service',
                'version': '1.0'
            }
        }
        
        # Serialize and produce
        topic = 'search-events'
        message = json.dumps(event_data, default=str)
        
        self.producer.produce(
            topic=topic,
            key=session_id.encode('utf-8') if session_id else None,
            value=message.encode('utf-8'),
            callback=self._delivery_callback
        )
        
        self.producer.poll(0)
        
        return event_data['event_id']
    
    def produce_availability_update(
        self,
        property_id: int,
        date: str,
        is_available: bool,
        price: float,
        reason: str
    ) -> str:
        """Produce availability update event"""
        return self.produce_event(
            event_type=EventType.AVAILABILITY_UPDATED,
            payload={
                'property_id': property_id,
                'date': date,
                'is_available': is_available,
                'price': price,
                'minimum_nights': 1,
                'reason': reason
            },
            property_id=property_id,
            metadata={
                'source': 'availability_service',
                'version': '1.0'
            }
        )
    
    def flush(self, timeout: int = 10):
        """Flush pending messages"""
        remaining = self.producer.flush(timeout)
        
        if remaining > 0:
            logger.warning(f"⚠️ {remaining} messages still in queue after flush")
        else:
            logger.info("✅ All messages flushed successfully")
    
    def close(self):
        """Close producer and flush remaining messages"""
        logger.info("Closing Kafka producer...")
        self.flush()
        logger.info("✅ Kafka producer closed")


# Example usage
if __name__ == "__main__":
    producer = AirStayEventProducer()
    
    # Example 1: Booking created event
    booking_data = {
        'booking_id': 12345,
        'property_id': 101,
        'guest_id': 501,
        'host_id': 201,
        'check_in_date': '2024-06-15',
        'check_out_date': '2024-06-18',
        'nights': 3,
        'num_guests': 2,
        'base_price': 150.00,
        'cleaning_fee': 25.00,
        'service_fee': 21.00,
        'total_price': 196.00,
        'booking_status': 'confirmed',
        'booking_channel': 'web'
    }
    
    event_id = producer.produce_booking_event(
        event_type=EventType.BOOKING_CREATED,
        booking_data=booking_data
    )
    
    print(f"✅ Booking event produced: {event_id}")
    
    # Example 2: Search event
    search_data = {
        'search_id': str(uuid.uuid4()),
        'city': 'New York',
        'check_in_date': '2024-07-01',
        'check_out_date': '2024-07-05',
        'num_guests': 2,
        'min_bedrooms': 1,
        'results_count': 45,
        'search_duration_ms': 123
    }
    
    search_event_id = producer.produce_search_event(
        search_data=search_data,
        user_id=501,
        session_id='session-abc123'
    )
    
    print(f"✅ Search event produced: {search_event_id}")
    
    # Flush and close
    producer.close()