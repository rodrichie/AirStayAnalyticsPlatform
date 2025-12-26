# Performance Optimization Guide

## Database Optimizations

### 1. Index Optimization

```sql
-- Property search indexes (already created)
CREATE INDEX idx_properties_city_active ON silver.properties(location_city, is_active);
CREATE INDEX idx_properties_type_active ON silver.properties(property_type, is_active);
CREATE INDEX idx_properties_price_range ON silver.properties(base_price) WHERE is_active = TRUE;

-- Booking query indexes
CREATE INDEX idx_bookings_guest_status ON silver.bookings(guest_id, booking_status);
CREATE INDEX idx_bookings_property_dates ON silver.bookings(property_id, check_in_date, check_out_date);
CREATE INDEX idx_bookings_created ON silver.bookings(created_at DESC);

-- Review indexes
CREATE INDEX idx_reviews_property_created ON silver.reviews(property_id, created_at DESC);
CREATE INDEX idx_reviews_sentiment ON silver.reviews(property_id, sentiment_label);

-- Availability calendar indexes
CREATE INDEX idx_availability_property_date ON silver.availability_calendar(property_id, calendar_date);
CREATE INDEX idx_availability_date_available ON silver.availability_calendar(calendar_date, is_available);
```

### 2. Query Optimization

**Use EXPLAIN ANALYZE:**

```sql
EXPLAIN ANALYZE
SELECT * FROM silver.properties
WHERE location_city = 'New York'
AND is_active = TRUE
LIMIT 20;
```

**Optimize aggregations:**

```sql
-- Bad: Multiple subqueries
SELECT p.*, 
    (SELECT COUNT(*) FROM bookings WHERE property_id = p.property_id),
    (SELECT AVG(rating) FROM reviews WHERE property_id = p.property_id)
FROM properties p;

-- Good: JOINs with GROUP BY
SELECT 
    p.*,
    COUNT(DISTINCT b.booking_id) as booking_count,
    AVG(r.rating) as avg_rating
FROM properties p
LEFT JOIN bookings b ON p.property_id = b.property_id
LEFT JOIN reviews r ON p.property_id = r.property_id
GROUP BY p.property_id;
```

### 3. Connection Pooling

**SQLAlchemy configuration:**

```python
engine = create_engine(
    DATABASE_URL,
    pool_size=20,           # Connections to keep in pool
    max_overflow=10,        # Additional connections when needed
    pool_pre_ping=True,     # Verify connections before use
    pool_recycle=3600,      # Recycle connections after 1 hour
    echo_pool=True          # Log pool events (debug only)
)
```

## Redis Caching Strategy

### 1. Cache Key Design

```python
# Hierarchical keys
cache_key = f"api:search:{city}:{bedrooms}:{price_hash}"

# Include version for cache invalidation
cache_key = f"v1:property:{property_id}"
```

### 2. TTL Strategy

```python
CACHE_TTL = {
    'static_data': 86400,        # 24 hours (property details)
    'semi_static': 3600,         # 1 hour (availability)
    'dynamic': 300,              # 5 minutes (search results)
    'real_time': 60,             # 1 minute (metrics)
}
```

### 3. Cache Warming

```python
# Pre-populate cache for popular queries
def warm_cache():
    for city in POPULAR_CITIES:
        search_properties(city=city, page=1)
        
    for property_id in TOP_PROPERTIES:
        get_property(property_id)
```

## API Performance

### 1. Response Compression

```python
# Already enabled in FastAPI
app.add_middleware(GZipMiddleware, minimum_size=1000)
```

### 2. Async Database Queries

```python
# Use async SQLAlchemy for I/O-bound operations
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

async_engine = create_async_engine(
    "postgresql+asyncpg://...",
    pool_size=20
)

@router.get("/properties")
async def get_properties(db: AsyncSession = Depends(get_async_db)):
    result = await db.execute(query)
    return result.all()
```

### 3. Response Pagination

```python
# Always paginate large result sets
MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 20

# Cursor-based pagination for large datasets
cursor = request.args.get('cursor')
query = query.filter(Property.id > cursor).limit(page_size)
```

## Kafka & Streaming Optimizations

### 1. Batch Processing

```python
# Producer batching
producer_config = {
    'linger.ms': 10,           # Wait 10ms to batch messages
    'batch.size': 32768,       # 32KB batches
    'compression.type': 'snappy'
}
```

### 2. Consumer Optimization

```python
# Consumer batching
consumer_config = {
    'max.poll.records': 500,   # Fetch up to 500 records
    'fetch.min.bytes': 1024,   # Wait for 1KB minimum
    'fetch.max.wait.ms': 500   # Max wait 500ms
}
```

## Spark Performance

### 1. Partition Tuning

```python
# Repartition for optimal parallelism
df = df.repartition(200)  # Rule: 2-4x number of cores

# Coalesce to reduce partitions after filtering
df = df.filter(...).coalesce(50)
```

### 2. Caching

```python
# Cache frequently accessed DataFrames
df.cache()
df.count()  # Trigger caching

# Unpersist when done
df.unpersist()
```

### 3. Broadcast Joins

```python
from pyspark.sql.functions import broadcast

# Broadcast small dimension tables
result = large_df.join(
    broadcast(small_df),
    on='key'
)
```

## Monitoring & Metrics

### 1. Application Metrics

```python
# Prometheus metrics
from prometheus_client import Counter, Histogram

request_count = Counter('api_requests_total', 'Total requests')
request_duration = Histogram('api_request_duration_seconds', 'Request duration')

@request_duration.time()
def handle_request():
    request_count.inc()
    # ... handle request
```

### 2. Database Metrics

```sql
-- Monitor slow queries
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    max_time
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 20;

-- Monitor table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### 3. Redis Metrics

```bash
# Monitor Redis performance
redis-cli INFO stats
redis-cli INFO memory
redis-cli SLOWLOG GET 10
```

## Load Balancing

### 1. API Load Balancing (Nginx)

```nginx
upstream api_backend {
    least_conn;  # Least connections algorithm
    
    server api1:8000 weight=3;
    server api2:8000 weight=3;
    server api3:8000 weight=2;
    
    keepalive 32;
}

server {
    location /api {
        proxy_pass http://api_backend;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
    }
}
```

### 2. Database Read Replicas

```python
# Master for writes
MASTER_DB = "postgresql://master:5432/airstay"

# Replicas for reads
READ_REPLICAS = [
    "postgresql://replica1:5432/airstay",
    "postgresql://replica2:5432/airstay",
]

# Route reads to replicas
def get_read_connection():
    return random.choice(READ_REPLICAS)
```

## Expected Performance Benchmarks

| Metric | Target | Acceptable | Poor |
|--------|--------|------------|------|
| API Response Time (p95) | <200ms | <500ms | >1s |
| API Response Time (p99) | <500ms | <1s | >2s |
| Database Query Time | <50ms | <200ms | >500ms |
| Cache Hit Rate | >80% | >60% | <50% |
| Throughput | >1000 RPS | >500 RPS | <200 RPS |
| Error Rate | <0.1% | <1% | >5% |

## Performance Testing Checklist

- [ ] Database indexes created and analyzed
- [ ] Connection pooling configured
- [ ] Redis caching implemented with appropriate TTLs
- [ ] API responses compressed (GZip)
- [ ] Query pagination implemented
- [ ] Load tests passed at expected traffic levels
- [ ] Monitoring and alerting configured
- [ ] Database query performance analyzed with EXPLAIN
- [ ] Spark jobs optimized with caching/partitioning
- [ ] Kafka batching configured
