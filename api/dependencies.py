"""
FastAPI Dependencies
Database connections, caching, authentication, etc.
"""
import logging
from typing import Generator, Optional
from functools import wraps
import hashlib
import json

from fastapi import Depends, HTTPException, Header, status
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
import redis

from api.config import get_settings, Settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Database setup
settings = get_settings()

engine = create_engine(
    settings.DATABASE_URL,
    poolclass=QueuePool,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_pre_ping=True,
    echo=settings.DEBUG
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Redis setup
redis_client = redis.from_url(
    settings.REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5
)

def get_db() -> Generator[Session, None, None]:
    """
    Get database session
    
    Yields:
        SQLAlchemy session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_redis() -> redis.Redis:
    """Get Redis client"""
    return redis_client     

def get_cache_key(prefix: str, **kwargs) -> str:
    """
    Generate cache key from parameters
    
    Args:
        prefix: Key prefix
        **kwargs: Parameters to include in key
        
    Returns:
        Cache key string
    """ 
    # Sort parameters for consistent keys
    params = sorted(kwargs.items())
    params_str = json.dumps(params, sort_keys=True)

    # Hash for shorter keys
    hash_suffix = hashlib.md5(params_str.encode()).hexdigest()[:8]

    return f"{prefix}:{hash_suffix}"

def cache_response(ttl: int = None):
    """
    Decorator to cache API responses in Redis
    
    Args:
        ttl: Time to live in seconds (None = use default)
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Skip cache if disabled
            if not settings.CACHE_ENABLED:
                return await func(*args, **kwargs)
            
            # Generate cache key
            cache_key = get_cache_key(
                prefix=f"api:{func.__name__}",
                **{k: str(v) for k, v in kwargs.items() if k != 'db' and k != 'redis'}
            ) 

            # Try to get from cache
            try:
                cached = redis_client.get(cache_key)
                if cached:
                    logger.debug(f"Cache hit: {cache_key}")
                    return json.loads(cached)
            except Exception as e:
                logger.warning(f"Cache read error: {e}")

            # Execute function
            result = await func(*args, **kwargs)

            # Store in cache
            try:
                cache_ttl = ttl or settings.CACHE_TTL
                redis_client.setex(
                    cache_key,
                    cache_ttl,
                    json.dumps(result, default=str)
                )
                logger.debug(f"Cache set: {cache_key} (TTL: {cache_ttl}s)")
            except Exception as e:
                logger.warning(f"Cache write error: {e}")
            
            return result    
        return wrapper
    return decorator

async def verify_api_key(
    x_api_key: Optional[str] = Header(None)
) -> str:
    """
    Verify API key (simplified - in production use proper auth)
    
    Args:
        x_api_key: API key from header
        
    Returns:
        Verified API key
        
    Raises:
        HTTPException if invalid
    """
    # For demo, accept any key or no key
    # In production, validate against database
    if settings.DEBUG:
        return x_api_key or "development"
    
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required"
        )
    
    # Validate key (placeholder)
    # In production: check against database, rate limits, etc.
    
    return x_api_key

class Paginator:
    """Pagination helper"""
    def __init__(
        self,
        page: int = 1,
        page_size: int = None
    ):
        """
        Initialize paginator
        
        Args:
            page: Page number (1-indexed)
            page_size: Items per page
        """
        self.page = max(1, page)
        self.page_size = min(
            page_size or settings.DEFAULT_PAGE_SIZE,
            settings.MAX_PAGE_SIZE
        )
        self.offset = (self.page - 1) * self.page_size
        self.limit = self.page_size

    def paginate_query(self, query):
        """Apply pagination to SQLAlchemy query"""
        return query.offset(self.offset).limit(self.limit)

    def get_metadata(self, total_count: int) -> dict:
        """Get pagination metadata"""
        total_pages = (total_count + self.page_size - 1) // self.page_size
        
        return {
            'page': self.page,
            'page_size': self.page_size,
            'total_count': total_count,
            'total_pages': total_pages,
            'has_next': self.page < total_pages,
            'has_previous': self.page > 1
        }    