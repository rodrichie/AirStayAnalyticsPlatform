"""
Generate sample data for AirStay Analytics Platform
Creates realistic properties, bookings, and reviews
"""
import random
import json
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
from psycopg2.extras import execute_values
import os

fake = Faker()

# Cities with coordinates
CITIES = [
    {"name": "New York", "state": "NY", "lat": 40.7128, "lon": -74.0060},
    {"name": "Los Angeles", "state": "CA", "lat": 34.0522, "lon": -118.2437},
    {"name": "San Francisco", "state": "CA", "lat": 37.7749, "lon": -122.4194},
    {"name": "Chicago", "state": "IL", "lat": 41.8781, "lon": -87.6298},
    {"name": "Miami", "state": "FL", "lat": 25.7617, "lon": -80.1918},
    {"name": "Seattle", "state": "WA", "lat": 47.6062, "lon": -122.3321},
    {"name": "Austin", "state": "TX", "lat": 30.2672, "lon": -97.7431},
    {"name": "Boston", "state": "MA", "lat": 42.3601, "lon": -71.0589},
]

PROPERTY_TYPES = [
    "Entire apartment", "Entire house", "Private room", 
    "Entire villa", "Entire condo", "Shared room"
]

AMENITIES = [
    "WiFi", "Kitchen", "Washer", "Dryer", "Air conditioning", 
    "Heating", "TV", "Parking", "Pool", "Gym", "Hot tub",
    "Workspace", "Pets allowed", "Self check-in"
]

CANCELLATION_POLICIES = ["Flexible", "Moderate", "Strict"]

def connect_db():
    """Connect to PostgreSQL"""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "airstay_db"),
        user=os.getenv("POSTGRES_USER", "airstay"),
        password=os.getenv("POSTGRES_PASSWORD", "airstay_pass")
    )

def generate_hosts(n=100):
    """Generate host data"""
    hosts = []
    for i in range(1, n + 1):
        host = {
            'host_id': i,
            'host_name': fake.name(),
            'host_since': fake.date_between(start_date='-5y', end_date='today'),
            'host_location': f"{random.choice(CITIES)['name']}, USA",
            'host_response_rate': round(random.uniform(50, 100), 2),
            'host_acceptance_rate': round(random.uniform(60, 100), 2),
            'is_superhost': random.choice([True, False]),
            'profile_picture_url': fake.image_url(),
            'about': fake.text(max_nb_chars=200),
            'total_listings': random.randint(1, 10),
            'verified_email': True,
            'verified_phone': random.choice([True, False])
        }
        hosts.append(host)
    return hosts

def generate_properties(n=500, hosts=None):
    """Generate property listings"""
    properties = []
    
    for i in range(1, n + 1):
        city = random.choice(CITIES)
        property_type = random.choice(PROPERTY_TYPES)
        
        # Add some randomness to coordinates (within city)
        lat = city['lat'] + random.uniform(-0.1, 0.1)
        lon = city['lon'] + random.uniform(-0.1, 0.1)
        
        bedrooms = random.randint(1, 5) if "room" not in property_type.lower() else 1
        bathrooms = round(random.uniform(1, bedrooms + 0.5), 1)
        max_guests = bedrooms * 2
        
        base_price = round(random.uniform(50, 500), 2)
        if city['name'] in ["New York", "San Francisco", "Los Angeles"]:
            base_price *= 1.5
        
        property_data = {
            'listing_id': f"L{str(i).zfill(6)}",
            'host_id': random.randint(1, len(hosts)) if hosts else random.randint(1, 100),
            'property_type': property_type,
            'title': f"{property_type} in {city['name']}",
            'description': fake.text(max_nb_chars=500),
            'bedrooms': bedrooms,
            'bathrooms': bathrooms,
            'beds': bedrooms + random.randint(0, 2),
            'max_guests': max_guests,
            'base_price': base_price,
            'cleaning_fee': round(base_price * 0.15, 2),
            'amenities': random.sample(AMENITIES, k=random.randint(5, 12)),
            'location_city': city['name'],
            'location_state': city['state'],
            'location_country': 'USA',
            'latitude': lat,
            'longitude': lon,
            'property_images': [fake.image_url() for _ in range(random.randint(3, 10))],
            'instant_bookable': random.choice([True, False]),
            'minimum_nights': random.choice([1, 2, 3, 7]),
            'maximum_nights': random.choice([30, 60, 90, 365]),
            'cancellation_policy': random.choice(CANCELLATION_POLICIES),
            'response_time_minutes': random.randint(10, 1440),
            'host_is_superhost': random.choice([True, False]),
            'property_rating': round(random.uniform(3.5, 5.0), 2),
            'review_count': random.randint(0, 200),
            'quality_score': round(random.uniform(0.6, 1.0), 2),
            'is_active': True
        }
        
        properties.append(property_data)
    
    return properties

def generate_bookings(n=2000, properties=None):
    """Generate booking data"""
    if not properties:
        return []
    
    bookings = []
    booking_statuses = ['confirmed', 'completed', 'canceled']
    
    for i in range(1, n + 1):
        property_data = random.choice(properties)
        
        # Random check-in date (past 180 days or future 90 days)
        days_offset = random.randint(-180, 90)
        check_in = datetime.now() + timedelta(days=days_offset)
        nights = random.randint(1, 14)
        check_out = check_in + timedelta(days=nights)
        
        status = random.choice(booking_statuses)
        
        base_price = property_data['base_price'] * nights
        cleaning_fee = property_data['cleaning_fee']
        service_fee = base_price * 0.12
        total_price = base_price + cleaning_fee + service_fee
        
        booking = {
            'property_id': property_data['property_id'],
            'guest_id': random.randint(1, 5000),
            'check_in_date': check_in.date(),
            'check_out_date': check_out.date(),
            'num_guests': random.randint(1, property_data['max_guests']),
            'base_price': round(base_price, 2),
            'cleaning_fee': round(cleaning_fee, 2),
            'service_fee': round(service_fee, 2),
            'total_price': round(total_price, 2),
            'booking_status': status,
            'booking_channel': random.choice(['website', 'mobile_app', 'phone']),
            'confirmed_at': check_in - timedelta(days=random.randint(1, 30)) if status != 'canceled' else None,
            'canceled_at': check_in - timedelta(days=random.randint(1, 10)) if status == 'canceled' else None
        }
        
        bookings.append(booking)
    
    return bookings

def load_hosts(conn, hosts):
    """Load hosts into database"""
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO silver.hosts (
            host_id, host_name, host_since, host_location, 
            host_response_rate, host_acceptance_rate, is_superhost,
            profile_picture_url, about, total_listings, 
            verified_email, verified_phone
        ) VALUES %s
        ON CONFLICT (host_id) DO NOTHING
    """
    
    values = [
        (
            h['host_id'], h['host_name'], h['host_since'], h['host_location'],
            h['host_response_rate'], h['host_acceptance_rate'], h['is_superhost'],
            h['profile_picture_url'], h['about'], h['total_listings'],
            h['verified_email'], h['verified_phone']
        )
        for h in hosts
    ]
    
    execute_values(cursor, insert_query, values)
    conn.commit()
    cursor.close()
    
    print(f"✅ Loaded {len(hosts)} hosts")

def load_properties(conn, properties):
    """Load properties into database"""
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO silver.properties (
            listing_id, host_id, property_type, title, description,
            bedrooms, bathrooms, beds, max_guests, base_price, cleaning_fee,
            amenities, location_city, location_state, location_country,
            latitude, longitude, location_point, property_images,
            instant_bookable, minimum_nights, maximum_nights, cancellation_policy,
            response_time_minutes, host_is_superhost, property_rating,
            review_count, quality_score, is_active
        ) VALUES %s
        RETURNING property_id, listing_id
    """
    
    values = [
        (
            p['listing_id'], p['host_id'], p['property_type'], p['title'], p['description'],
            p['bedrooms'], p['bathrooms'], p['beds'], p['max_guests'], p['base_price'], p['cleaning_fee'],
            p['amenities'], p['location_city'], p['location_state'], p['location_country'],
            p['latitude'], p['longitude'], f"POINT({p['longitude']} {p['latitude']})",
            p['property_images'], p['instant_bookable'], p['minimum_nights'], p['maximum_nights'],
            p['cancellation_policy'], p['response_time_minutes'], p['host_is_superhost'],
            p['property_rating'], p['review_count'], p['quality_score'], p['is_active']
        )
        for p in properties
    ]
    
    execute_values(cursor, insert_query, values)
    
    # Get generated property IDs
    cursor.execute("SELECT property_id, listing_id FROM silver.properties ORDER BY property_id")
    id_mapping = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Update properties with actual IDs
    for p in properties:
        p['property_id'] = id_mapping[p['listing_id']]
    
    conn.commit()
    cursor.close()
    
    print(f"✅ Loaded {len(properties)} properties")
    return properties

def load_bookings(conn, bookings):
    """Load bookings into database"""
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO silver.bookings (
            property_id, guest_id, check_in_date, check_out_date,
            num_guests, base_price, cleaning_fee, service_fee, total_price,
            booking_status, booking_channel, confirmed_at, canceled_at
        ) VALUES %s
    """
    
    values = [
        (
            b['property_id'], b['guest_id'], b['check_in_date'], b['check_out_date'],
            b['num_guests'], b['base_price'], b['cleaning_fee'], b['service_fee'], b['total_price'],
            b['booking_status'], b['booking_channel'], b['confirmed_at'], b['canceled_at']
        )
        for b in bookings
    ]
    
    execute_values(cursor, insert_query, values)
    conn.commit()
    cursor.close()
    
    print(f"✅ Loaded {len(bookings)} bookings")

def main():
    """Main execution"""
    print("🚀 Generating sample data for AirStay Analytics...")
    
    # Connect to database
    conn = connect_db()
    
    # Generate data
    print("\n📊 Generating hosts...")
    hosts = generate_hosts(n=100)
    load_hosts(conn, hosts)
    
    print("\n🏠 Generating properties...")
    properties = generate_properties(n=500, hosts=hosts)
    properties = load_properties(conn, properties)
    
    print("\n📅 Generating bookings...")
    bookings = generate_bookings(n=2000, properties=properties)
    load_bookings(conn, bookings)
    
    conn.close()
    
    print("\n✅ Sample data generation complete!")
    print(f"   - Hosts: {len(hosts)}")
    print(f"   - Properties: {len(properties)}")
    print(f"   - Bookings: {len(bookings)}")

if __name__ == "__main__":
    main()