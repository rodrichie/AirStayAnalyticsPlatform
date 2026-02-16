"""
FastAPI Main Application
AirStay Analytics API
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
import time

from api.config import get_settings
from api.routers import (
    properties,
    bookings,
    pricing,
    recommendations,
    analytics
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    logger.info("Starting AirStay Analytics API")
    logger.info(f"   Environment: {'Development' if settings.DEBUG else 'Production'}")
    logger.info(f"   Version: {settings.APP_VERSION}")

    # Test database connection
    try:
        from sqlalchemy import text as sa_text
        from api.dependencies import engine
        with engine.connect() as conn:
            conn.execute(sa_text("SELECT 1"))
        logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")

    # Test Redis connection
    try:
        from api.dependencies import redis_client
        redis_client.ping()
        logger.info("Redis connection successful")
    except Exception as e:
        logger.warning(f"Redis connection failed (caching disabled): {e}")

    yield

    # Shutdown
    logger.info("Shutting down AirStay Analytics API")


# OpenAPI tag metadata
tags_metadata = [
    {
        "name": "Properties",
        "description": "Search, filter, and retrieve vacation rental properties. Supports geo-spatial queries, availability calendars, similar property lookups, and paginated reviews.",
    },
    {
        "name": "Bookings",
        "description": "Create, retrieve, and cancel bookings. Validates property availability, guest capacity, and minimum night requirements before confirming reservations.",
    },
    {
        "name": "Pricing",
        "description": "ML-driven dynamic pricing recommendations. Uses historical demand patterns, seasonal trends, competitor pricing, and property features to suggest optimal nightly rates.",
    },
    {
        "name": "Recommendations",
        "description": "Personalized property recommendations powered by collaborative filtering and content-based models. Includes user-specific suggestions, similar property matching, and trending destinations.",
    },
    {
        "name": "Analytics",
        "description": "Business intelligence and performance metrics. Property-level performance over time, city-level aggregations, real-time platform activity, and executive dashboard summaries.",
    },
]

# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description=(
        "## Vacation Rental Analytics Platform\n\n"
        "AirStay Analytics is a production-grade platform for managing and analyzing "
        "vacation rental data with ML-powered pricing and recommendation engines.\n\n"
        "### Architecture\n\n"
        "- **PostgreSQL** data warehouse with silver/gold medallion layers\n"
        "- **Redis** caching for low-latency responses (TTL 60s to 30min)\n"
        "- **Apache Kafka** for real-time booking and search event streaming\n"
        "- **Apache Spark** for batch analytics and stream processing\n"
        "- **Apache Airflow** for orchestrating ETL and ML training pipelines\n"
        "- **Streamlit** dashboard for visual analytics\n\n"
        "### ML Models\n\n"
        "| Model | Purpose |\n"
        "|---|---|\n"
        "| Dynamic Pricing | Optimal nightly rate recommendations |\n"
        "| Demand Forecasting | Occupancy and revenue predictions |\n"
        "| Recommendation Engine | Personalized property suggestions |\n"
        "| Anomaly Detection | Unusual booking and pricing pattern alerts |\n"
        "| Sentiment Analysis | Review classification and scoring |\n"
    ),
    docs_url=f"{settings.API_PREFIX}/docs",
    redoc_url=f"{settings.API_PREFIX}/redoc",
    openapi_url=f"{settings.API_PREFIX}/openapi.json",
    openapi_tags=tags_metadata,
    contact={
        "name": "AirStay Engineering",
        "url": "https://github.com/rodrichie/AirStayAnalyticsPlatform",
    },
    license_info={
        "name": "MIT",
    },
    lifespan=lifespan
)

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)


# Request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Add processing time to response headers"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(round(process_time * 1000, 2))
    return response


# Exception handlers
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors"""
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "success": False,
            "error": "Validation Error",
            "detail": exc.errors()
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "success": False,
            "error": "Internal Server Error",
            "detail": str(exc) if settings.DEBUG else "An error occurred"
        }
    )


# Include routers
app.include_router(properties.router, prefix=settings.API_PREFIX)
app.include_router(bookings.router, prefix=settings.API_PREFIX)
app.include_router(pricing.router, prefix=settings.API_PREFIX)
app.include_router(recommendations.router, prefix=settings.API_PREFIX)
app.include_router(analytics.router, prefix=settings.API_PREFIX)


# Health check endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": settings.APP_VERSION,
        "timestamp": time.time()
    }


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "AirStay Analytics API",
        "version": settings.APP_VERSION,
        "docs": f"{settings.API_PREFIX}/docs"
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        workers=4 if not settings.DEBUG else 1
    )
