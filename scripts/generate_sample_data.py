"""
Generate sample data for AirStay Analytics Platform.
Creates realistic hosts, properties, bookings, reviews, and gold-layer aggregates.
"""
import random
import time
import os
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
from psycopg2.extras import execute_values

fake = Faker()

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

REVIEW_TEMPLATES = [
    "Great place to stay! Very clean and the host was responsive.",
    "Good location, close to restaurants and shops. Would stay again.",
    "The apartment was exactly as described. Comfortable beds.",
    "Excellent value for money. The neighborhood was quiet and safe.",
    "Beautiful views from the balcony. Kitchen was well-equipped.",
    "Perfect for a family vacation. Kids loved the pool.",
    "Nice and cozy space. Check-in was smooth and easy.",
    "The host went above and beyond. Highly recommended.",
    "Decent place but could use some updates. Good for the price.",
    "Spacious and modern. Walking distance to public transit.",
    "Loved the decor. Very stylish and comfortable.",
    "Clean, quiet, and private. Everything you need for a short stay.",
    "Would definitely book again. Great communication with host.",
    "Central location made exploring the city very convenient.",
    "A hidden gem! Better than most hotels at this price point.",
]


def connect_db():
    """Connect to PostgreSQL with retries."""
    for attempt in range(30):
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", 5432)),
                database=os.getenv("POSTGRES_DB", "airstay_db"),
                user=os.getenv("POSTGRES_USER", "airstay"),
                password=os.getenv("POSTGRES_PASSWORD", "airstay_pass")
            )
            return conn
        except Exception as e:
            if attempt < 29:
                print(f"  Waiting for database... (attempt {attempt + 1})")
                time.sleep(2)
            else:
                raise e


def check_already_seeded(conn):
    """Check if data already exists."""
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM silver.properties")
    count = cursor.fetchone()[0]
    cursor.close()
    if count > 0:
        print(f"  Database already has {count} properties -- skipping seed.")
        return True
    return False


def generate_hosts(n=100):
    """Generate host data."""
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
            'profile_picture_url': f"https://example.com/hosts/{i}.jpg",
            'about': fake.text(max_nb_chars=200),
            'total_listings': random.randint(1, 10),
            'verified_email': True,
            'verified_phone': random.choice([True, False])
        }
        hosts.append(host)
    return hosts


def generate_properties(n=500, hosts=None):
    """Generate property listings."""
    properties = []
    neighborhoods = {
        "New York": ["Manhattan", "Brooklyn", "Queens", "Bronx", "Harlem"],
        "Los Angeles": ["Hollywood", "Venice", "Santa Monica", "Downtown", "Silver Lake"],
        "San Francisco": ["Mission", "SOMA", "Marina", "Castro", "Haight"],
        "Chicago": ["Loop", "Lincoln Park", "Wicker Park", "Hyde Park", "River North"],
        "Miami": ["South Beach", "Brickell", "Wynwood", "Coconut Grove", "Downtown"],
        "Seattle": ["Capitol Hill", "Ballard", "Fremont", "Queen Anne", "Belltown"],
        "Austin": ["Downtown", "East Austin", "South Congress", "Zilker", "Mueller"],
        "Boston": ["Back Bay", "Beacon Hill", "Cambridge", "South End", "Fenway"],
    }

    for i in range(1, n + 1):
        city = random.choice(CITIES)
        property_type = random.choice(PROPERTY_TYPES)
        lat = city['lat'] + random.uniform(-0.1, 0.1)
        lon = city['lon'] + random.uniform(-0.1, 0.1)
        bedrooms = random.randint(1, 5) if "room" not in property_type.lower() else 1
        bathrooms = round(random.uniform(1, bedrooms + 0.5), 1)
        max_guests = bedrooms * 2
        base_price = round(random.uniform(50, 500), 2)
        if city['name'] in ["New York", "San Francisco", "Los Angeles"]:
            base_price *= 1.5
        neighborhood = random.choice(neighborhoods.get(city['name'], ["Downtown"]))

        property_data = {
            'listing_id': f"L{str(i).zfill(6)}",
            'host_id': random.randint(1, len(hosts)) if hosts else random.randint(1, 100),
            'property_type': property_type,
            'title': f"{property_type} in {neighborhood}, {city['name']}",
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
            'latitude': round(lat, 7),
            'longitude': round(lon, 7),
            'neighborhood': neighborhood,
            'property_images': [f"https://example.com/properties/{i}_{j}.jpg" for j in range(random.randint(3, 8))],
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
    """Generate booking data."""
    if not properties:
        return []
    bookings = []
    booking_statuses = ['confirmed', 'completed', 'canceled']

    for i in range(1, n + 1):
        property_data = random.choice(properties)
        days_offset = random.randint(-180, 90)
        check_in = datetime.now() + timedelta(days=days_offset)
        nights = random.randint(1, 14)
        check_out = check_in + timedelta(days=nights)
        status = random.choices(
            booking_statuses,
            weights=[30, 50, 20],
        )[0]
        base_price = property_data['base_price'] * nights
        cleaning_fee = property_data['cleaning_fee']
        service_fee = base_price * 0.12
        total_price = base_price + cleaning_fee + service_fee
        created_at = check_in - timedelta(days=random.randint(1, 60))

        booking = {
            'property_id': property_data['property_id'],
            'guest_id': random.randint(1, 5000),
            'check_in_date': check_in.date(),
            'check_out_date': check_out.date(),
            'nights': nights,
            'num_guests': random.randint(1, property_data['max_guests']),
            'base_price': round(base_price, 2),
            'cleaning_fee': round(cleaning_fee, 2),
            'service_fee': round(service_fee, 2),
            'total_price': round(total_price, 2),
            'booking_status': status,
            'booking_channel': random.choice(['website', 'mobile_app', 'phone']),
            'created_at': created_at,
            'confirmed_at': created_at + timedelta(hours=random.randint(1, 24)) if status != 'canceled' else None,
            'canceled_at': check_in - timedelta(days=random.randint(1, 10)) if status == 'canceled' else None
        }
        bookings.append(booking)
    return bookings


def generate_reviews(bookings, properties_map):
    """Generate reviews for completed bookings."""
    reviews = []
    completed = [b for b in bookings if b['booking_status'] == 'completed']
    reviewed = random.sample(completed, k=min(len(completed), int(len(completed) * 0.7)))

    for b in reviewed:
        overall = random.randint(3, 5)
        sentiment_map = {5: ("positive", 0.9), 4: ("positive", 0.7), 3: ("neutral", 0.5)}
        label, score = sentiment_map.get(overall, ("positive", 0.8))

        review = {
            'property_id': b['property_id'],
            'booking_id': b.get('booking_id'),
            'guest_id': b['guest_id'],
            'rating': overall,
            'cleanliness_rating': min(5, max(1, overall + random.randint(-1, 1))),
            'accuracy_rating': min(5, max(1, overall + random.randint(-1, 0))),
            'communication_rating': min(5, max(1, overall + random.randint(-1, 1))),
            'location_rating': min(5, max(1, overall + random.randint(-1, 1))),
            'value_rating': min(5, max(1, overall + random.randint(-1, 0))),
            'review_text': random.choice(REVIEW_TEMPLATES),
            'review_language': 'en',
            'sentiment_score': round(score + random.uniform(-0.1, 0.1), 2),
            'sentiment_label': label,
            'created_at': b['created_at'] + timedelta(days=random.randint(1, 14)),
        }
        reviews.append(review)
    return reviews


def load_hosts(conn, hosts):
    """Load hosts into database."""
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
    print(f"  Loaded {len(hosts)} hosts")


def load_properties(conn, properties):
    """Load properties into database."""
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO silver.properties (
            listing_id, host_id, property_type, title, description,
            bedrooms, bathrooms, beds, max_guests, base_price, cleaning_fee,
            amenities, location_city, location_state, location_country,
            latitude, longitude, location_point, property_images,
            instant_bookable, minimum_nights, maximum_nights, cancellation_policy,
            response_time_minutes, host_is_superhost, property_rating,
            review_count, quality_score, is_active, neighborhood
        ) VALUES %s
    """
    values = [
        (
            p['listing_id'], p['host_id'], p['property_type'], p['title'], p['description'],
            p['bedrooms'], p['bathrooms'], p['beds'], p['max_guests'], p['base_price'], p['cleaning_fee'],
            p['amenities'], p['location_city'], p['location_state'], p['location_country'],
            p['latitude'], p['longitude'], f"POINT({p['longitude']} {p['latitude']})",
            p['property_images'], p['instant_bookable'], p['minimum_nights'], p['maximum_nights'],
            p['cancellation_policy'], p['response_time_minutes'], p['host_is_superhost'],
            p['property_rating'], p['review_count'], p['quality_score'], p['is_active'],
            p['neighborhood']
        )
        for p in properties
    ]
    execute_values(cursor, insert_query, values)

    cursor.execute("SELECT property_id, listing_id FROM silver.properties ORDER BY property_id")
    id_mapping = {row[1]: row[0] for row in cursor.fetchall()}
    for p in properties:
        p['property_id'] = id_mapping[p['listing_id']]

    conn.commit()
    cursor.close()
    print(f"  Loaded {len(properties)} properties")
    return properties


def load_bookings(conn, bookings):
    """Load bookings into database."""
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO silver.bookings (
            property_id, guest_id, check_in_date, check_out_date, nights,
            num_guests, base_price, cleaning_fee, service_fee, total_price,
            booking_status, booking_channel, created_at, confirmed_at, canceled_at
        ) VALUES %s
    """
    values = [
        (
            b['property_id'], b['guest_id'], b['check_in_date'], b['check_out_date'],
            b['nights'], b['num_guests'], b['base_price'], b['cleaning_fee'],
            b['service_fee'], b['total_price'], b['booking_status'],
            b['booking_channel'], b['created_at'], b['confirmed_at'], b['canceled_at']
        )
        for b in bookings
    ]
    execute_values(cursor, insert_query, values)

    # Fetch booking IDs for review generation
    cursor.execute("""
        SELECT booking_id, property_id, guest_id, booking_status, created_at
        FROM silver.bookings ORDER BY booking_id
    """)
    rows = cursor.fetchall()
    for i, b in enumerate(bookings):
        if i < len(rows):
            b['booking_id'] = rows[i][0]

    conn.commit()
    cursor.close()
    print(f"  Loaded {len(bookings)} bookings")
    return bookings


def load_reviews(conn, reviews):
    """Load reviews into database."""
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO silver.reviews (
            property_id, booking_id, guest_id, rating,
            cleanliness_rating, accuracy_rating, communication_rating,
            location_rating, value_rating, review_text,
            review_language, sentiment_score, sentiment_label, created_at
        ) VALUES %s
    """
    values = [
        (
            r['property_id'], r.get('booking_id'), r['guest_id'], r['rating'],
            r['cleanliness_rating'], r['accuracy_rating'], r['communication_rating'],
            r['location_rating'], r['value_rating'], r['review_text'],
            r['review_language'], r['sentiment_score'], r['sentiment_label'],
            r['created_at']
        )
        for r in reviews
    ]
    execute_values(cursor, insert_query, values)
    conn.commit()
    cursor.close()
    print(f"  Loaded {len(reviews)} reviews")


def populate_gold_layer(conn):
    """Populate gold-layer aggregate tables from silver data."""
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO gold.dim_hosts (host_id, host_name, host_since, is_superhost,
            response_rate, acceptance_rate, total_listings)
        SELECT host_id, host_name, host_since, is_superhost,
            host_response_rate, host_acceptance_rate, total_listings
        FROM silver.hosts
        ON CONFLICT (host_id) DO NOTHING
    """)
    print("  Populated gold.dim_hosts")

    cursor.execute("""
        INSERT INTO gold.dim_properties (property_id, listing_id, property_type, city,
            neighborhood, bedrooms, bathrooms, max_guests, latitude, longitude,
            amenities_count, is_instant_bookable, host_is_superhost, created_date)
        SELECT property_id, listing_id, property_type, location_city,
            neighborhood, bedrooms, bathrooms, max_guests, latitude, longitude,
            COALESCE(array_length(amenities, 1), 0), instant_bookable,
            host_is_superhost, created_at::DATE
        FROM silver.properties
        ON CONFLICT (property_id) DO NOTHING
    """)
    print("  Populated gold.dim_properties")

    cursor.execute("""
        INSERT INTO gold.agg_property_performance
            (property_id, metric_date, bookings_count, revenue_total, nights_booked,
             occupancy_rate, avg_nightly_rate, avg_rating, review_count, cancellation_count)
        SELECT
            b.property_id,
            DATE(b.created_at) as metric_date,
            COUNT(*) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')) as bookings_count,
            COALESCE(SUM(b.total_price) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')), 0) as revenue_total,
            COALESCE(SUM(b.nights) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')), 0) as nights_booked,
            ROUND(COALESCE(SUM(b.nights) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')), 0)::DECIMAL / 30 * 100, 2) as occupancy_rate,
            ROUND(AVG(p.base_price)::DECIMAL, 2) as avg_nightly_rate,
            ROUND(AVG(p.property_rating)::DECIMAL, 2) as avg_rating,
            (SELECT COUNT(*) FROM silver.reviews r WHERE r.property_id = b.property_id) as review_count,
            COUNT(*) FILTER (WHERE b.booking_status = 'canceled') as cancellation_count
        FROM silver.bookings b
        JOIN silver.properties p ON b.property_id = p.property_id
        GROUP BY b.property_id, DATE(b.created_at)
        ON CONFLICT (property_id, metric_date) DO NOTHING
    """)
    print("  Populated gold.agg_property_performance")

    cursor.execute("""
        INSERT INTO gold.agg_city_metrics
            (city, metric_date, active_properties, total_bookings, total_revenue,
             avg_occupancy_rate, avg_nightly_rate)
        SELECT
            p.location_city as city,
            DATE(b.created_at) as metric_date,
            COUNT(DISTINCT p.property_id) as active_properties,
            COUNT(*) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')) as total_bookings,
            COALESCE(SUM(b.total_price) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')), 0) as total_revenue,
            ROUND(AVG(COALESCE(b.nights, 0))::DECIMAL / 30 * 100, 2) as avg_occupancy_rate,
            ROUND(AVG(p.base_price)::DECIMAL, 2) as avg_nightly_rate
        FROM silver.bookings b
        JOIN silver.properties p ON b.property_id = p.property_id
        GROUP BY p.location_city, DATE(b.created_at)
        ON CONFLICT (city, metric_date) DO NOTHING
    """)
    print("  Populated gold.agg_city_metrics")

    cursor.execute("""
        INSERT INTO gold.pricing_recommendations
            (property_id, recommendation_date, current_price, recommended_price,
             price_change_pct, reason, confidence_score)
        SELECT
            p.property_id,
            CURRENT_DATE,
            p.base_price,
            ROUND((p.base_price * (0.9 + random() * 0.2))::DECIMAL, 2) as recommended_price,
            ROUND((random() * 20 - 10)::DECIMAL, 2) as price_change_pct,
            CASE
                WHEN random() < 0.3 THEN 'Market demand increase detected'
                WHEN random() < 0.6 THEN 'Seasonal adjustment recommended'
                ELSE 'Competitor pricing alignment'
            END as reason,
            ROUND((0.7 + random() * 0.3)::DECIMAL, 2) as confidence_score
        FROM silver.properties p
        WHERE p.is_active = TRUE
        ON CONFLICT (property_id, recommendation_date) DO NOTHING
    """)
    print("  Populated gold.pricing_recommendations")

    # Populate search trends
    cursor.execute("""
        INSERT INTO silver.search_trends_cities (city, metric_date, window_start, search_count, unique_users)
        SELECT
            city,
            d::DATE as metric_date,
            d as window_start,
            (50 + random() * 200)::INTEGER as search_count,
            (20 + random() * 80)::INTEGER as unique_users
        FROM (
            VALUES ('New York'), ('Los Angeles'), ('Miami'), ('San Francisco'),
                   ('Chicago'), ('Austin'), ('Denver'), ('Seattle')
        ) AS cities(city)
        CROSS JOIN generate_series(
            CURRENT_DATE - INTERVAL '14 days',
            CURRENT_DATE,
            INTERVAL '1 day'
        ) d
    """)
    print("  Populated silver.search_trends_cities")

    conn.commit()
    cursor.close()


def main():
    """Main execution."""
    print("=" * 50)
    print("  AIRSTAY ANALYTICS -- DATA SEEDER")
    print("=" * 50)

    conn = connect_db()

    if check_already_seeded(conn):
        return

    print("\n  Generating hosts...")
    hosts = generate_hosts(n=100)
    load_hosts(conn, hosts)

    print("  Generating properties...")
    properties = generate_properties(n=500, hosts=hosts)
    properties = load_properties(conn, properties)

    print("  Generating bookings...")
    bookings = generate_bookings(n=2000, properties=properties)
    bookings = load_bookings(conn, bookings)

    print("  Generating reviews...")
    properties_map = {p['property_id']: p for p in properties}
    reviews = generate_reviews(bookings, properties_map)
    load_reviews(conn, reviews)

    print("\n  Populating gold layer aggregates...")
    populate_gold_layer(conn)

    conn.close()

    print("\n  Sample data generation complete!")
    print(f"    Hosts:      {len(hosts)}")
    print(f"    Properties: {len(properties)}")
    print(f"    Bookings:   {len(bookings)}")
    print(f"    Reviews:    {len(reviews)}")
    print("=" * 50)


if __name__ == "__main__":
    main()
