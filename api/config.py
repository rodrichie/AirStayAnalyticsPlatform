"""
API Configuration
Centralized configuration for FastAPI application
"""
import os
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    """Application settings"""
    
    # Application
    APP_NAME: str = "AirStay Analytics API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # Database
    DATABASE_URL: str = "postgresql://airstay:airstay_pass@postgres:5432/airstay_db"
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 10

    # Redis
    REDIS_URL: str = "redis://redis:6379"
    CACHE_TTL: int = 300  # 5 minutes
    CACHE_ENABLED: bool = True

    # ML Models
    PRICING_MODEL_PATH: str = "/opt/airflow/models/pricing_model_latest.pkl"
    RECOMMENDATION_MODEL_PATH: str = "/opt/airflow/models/recommendation_model.pkl"
    ANOMALY_MODEL_PATH: str = "/opt/airflow/models/anomaly_detection_model.pkl"

    # API
    API_PREFIX: str = "/api/v1"
    CORS_ORIGINS: list = ["*"]
    RATE_LIMIT: int = 100  # requests per minute

    # Pagination
    DEFAULT_PAGE_SIZE: int = 20
    MAX_PAGE_SIZE: int = 100

    class Config:
        env_file = ".env"
        case_sensitive = True

@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()        