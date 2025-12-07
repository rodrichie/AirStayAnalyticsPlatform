#!/bin/bash
set -e

echo "🚀 Initializing AirStay Analytics Database..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Enable PostGIS extension
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS postgis_topology;
    
    -- Create additional databases
    CREATE DATABASE airflow_db;
    CREATE DATABASE mlflow_db;
    
    -- Create schemas
    CREATE SCHEMA IF NOT EXISTS bronze;
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE SCHEMA IF NOT EXISTS gold;
    CREATE SCHEMA IF NOT EXISTS metadata;
    
    -- ========================================
    -- BRONZE LAYER (Raw Data)
    -- ========================================
    
    CREATE TABLE IF NOT EXISTS bronze.raw_listings (
        id SERIAL PRIMARY KEY,
        listing_id VARCHAR(100),
        host_id VARCHAR(100),
        listing_json JSONB NOT NULL,
        images_json JSONB,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_system VARCHAR(50)
    );
    
    CREATE TABLE IF NOT EXISTS bronze.raw_bookings (
        id SERIAL PRIMARY KEY,
        booking_id VARCHAR(100),
        listing_id VARCHAR(100),
        guest_id VARCHAR(100),
        booking_json JSONB NOT NULL,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS bronze.raw_reviews (
        id SERIAL PRIMARY KEY,
        review_id VARCHAR(100),
        listing_id VARCHAR(100),
        guest_id VARCHAR(100),
        review_json JSONB NOT NULL,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- ========================================
    -- SILVER LAYER (Cleansed & Enriched)
    -- ========================================
    
    CREATE TABLE IF NOT EXISTS silver.properties (
        property_id BIGSERIAL PRIMARY KEY,
        listing_id VARCHAR(100) UNIQUE NOT NULL,
        host_id BIGINT,
        property_type VARCHAR(100),
        title VARCHAR(500),
        description TEXT,
        bedrooms INTEGER,
        bathrooms DECIMAL(3,1),
        beds INTEGER,
        max_guests INTEGER,
        base_price DECIMAL(10,2),
        cleaning_fee DECIMAL(10,2),
        amenities TEXT[],
        house_rules TEXT[],
        location_address VARCHAR(500),
        location_city VARCHAR(100),
        location_state VARCHAR(100),
        location_country VARCHAR(100),
        location_zipcode VARCHAR(20),
        location_point GEOGRAPHY(POINT, 4326),
        neighborhood VARCHAR(255),
        latitude DECIMAL(10,7),
        longitude DECIMAL(10,7),
        property_images TEXT[],
        instant_bookable BOOLEAN DEFAULT FALSE,
        minimum_nights INTEGER DEFAULT 1,
        maximum_nights INTEGER DEFAULT 365,
        cancellation_policy VARCHAR(50),
        response_time_minutes INTEGER,
        host_is_superhost BOOLEAN DEFAULT FALSE,
        property_rating DECIMAL(3,2),
        review_count INTEGER DEFAULT 0,
        quality_score DECIMAL(3,2),
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX idx_properties_location ON silver.properties USING GIST(location_point);
    CREATE INDEX idx_properties_city ON silver.properties(location_city);
    CREATE INDEX idx_properties_type ON silver.properties(property_type);
    
    CREATE TABLE IF NOT EXISTS silver.bookings (
        booking_id BIGSERIAL PRIMARY KEY,
        property_id BIGINT REFERENCES silver.properties(property_id),
        guest_id BIGINT,
        check_in_date DATE NOT NULL,
        check_out_date DATE NOT NULL,
        nights INTEGER GENERATED ALWAYS AS (check_out_date - check_in_date) STORED,
        num_guests INTEGER,
        base_price DECIMAL(10,2),
        cleaning_fee DECIMAL(10,2),
        service_fee DECIMAL(10,2),
        total_price DECIMAL(10,2),
        booking_status VARCHAR(50),
        booking_channel VARCHAR(50),
        special_requests TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        confirmed_at TIMESTAMP,
        canceled_at TIMESTAMP,
        cancellation_reason VARCHAR(255)
    );
    
    CREATE INDEX idx_bookings_property ON silver.bookings(property_id);
    CREATE INDEX idx_bookings_dates ON silver.bookings(check_in_date, check_out_date);
    CREATE INDEX idx_bookings_status ON silver.bookings(booking_status);
    
    CREATE TABLE IF NOT EXISTS silver.reviews (
        review_id BIGSERIAL PRIMARY KEY,
        property_id BIGINT REFERENCES silver.properties(property_id),
        booking_id BIGINT REFERENCES silver.bookings(booking_id),
        guest_id BIGINT,
        rating INTEGER CHECK (rating BETWEEN 1 AND 5),
        cleanliness_rating INTEGER CHECK (cleanliness_rating BETWEEN 1 AND 5),
        accuracy_rating INTEGER CHECK (accuracy_rating BETWEEN 1 AND 5),
        communication_rating INTEGER CHECK (communication_rating BETWEEN 1 AND 5),
        location_rating INTEGER CHECK (location_rating BETWEEN 1 AND 5),
        value_rating INTEGER CHECK (value_rating BETWEEN 1 AND 5),
        review_text TEXT,
        review_language VARCHAR(10),
        sentiment_score DECIMAL(3,2),
        sentiment_label VARCHAR(20),
        host_response TEXT,
        host_response_time_hours INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX idx_reviews_property ON silver.reviews(property_id);
    CREATE INDEX idx_reviews_rating ON silver.reviews(rating);
    
    CREATE TABLE IF NOT EXISTS silver.availability_calendar (
        id BIGSERIAL PRIMARY KEY,
        property_id BIGINT REFERENCES silver.properties(property_id),
        calendar_date DATE NOT NULL,
        is_available BOOLEAN DEFAULT TRUE,
        price DECIMAL(10,2),
        minimum_nights INTEGER,
        maximum_nights INTEGER,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(property_id, calendar_date)
    );
    
    CREATE INDEX idx_availability_property_date ON silver.availability_calendar(property_id, calendar_date);
    
    CREATE TABLE IF NOT EXISTS silver.hosts (
        host_id BIGSERIAL PRIMARY KEY,
        host_name VARCHAR(255),
        host_since DATE,
        host_location VARCHAR(255),
        host_response_rate DECIMAL(5,2),
        host_acceptance_rate DECIMAL(5,2),
        is_superhost BOOLEAN DEFAULT FALSE,
        profile_picture_url VARCHAR(500),
        about TEXT,
        total_listings INTEGER DEFAULT 0,
        verified_email BOOLEAN DEFAULT FALSE,
        verified_phone BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- ========================================
    -- GOLD LAYER (Analytics-Ready)
    -- ========================================
    
    CREATE TABLE IF NOT EXISTS gold.dim_properties (
        property_key BIGSERIAL PRIMARY KEY,
        property_id BIGINT UNIQUE NOT NULL,
        listing_id VARCHAR(100),
        property_type VARCHAR(100),
        city VARCHAR(100),
        neighborhood VARCHAR(255),
        bedrooms INTEGER,
        bathrooms DECIMAL(3,1),
        max_guests INTEGER,
        latitude DECIMAL(10,7),
        longitude DECIMAL(10,7),
        amenities_count INTEGER,
        is_instant_bookable BOOLEAN,
        host_is_superhost BOOLEAN,
        created_date DATE,
        scd_start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        scd_end_date TIMESTAMP,
        is_current BOOLEAN DEFAULT TRUE
    );
    
    CREATE TABLE IF NOT EXISTS gold.dim_hosts (
        host_key BIGSERIAL PRIMARY KEY,
        host_id BIGINT UNIQUE NOT NULL,
        host_name VARCHAR(255),
        host_since DATE,
        is_superhost BOOLEAN,
        response_rate DECIMAL(5,2),
        acceptance_rate DECIMAL(5,2),
        total_listings INTEGER
    );
    
    CREATE TABLE IF NOT EXISTS gold.dim_dates (
        date_key INTEGER PRIMARY KEY,
        calendar_date DATE UNIQUE NOT NULL,
        day_of_week INTEGER,
        day_name VARCHAR(20),
        week_of_year INTEGER,
        month INTEGER,
        month_name VARCHAR(20),
        quarter INTEGER,
        year INTEGER,
        is_weekend BOOLEAN,
        is_holiday BOOLEAN,
        season VARCHAR(20)
    );
    
    CREATE TABLE IF NOT EXISTS gold.fact_bookings (
        booking_key BIGSERIAL PRIMARY KEY,
        booking_id BIGINT UNIQUE NOT NULL,
        property_key BIGINT REFERENCES gold.dim_properties(property_key),
        host_key BIGINT REFERENCES gold.dim_hosts(host_key),
        check_in_date_key INTEGER REFERENCES gold.dim_dates(date_key),
        check_out_date_key INTEGER REFERENCES gold.dim_dates(date_key),
        booking_date_key INTEGER REFERENCES gold.dim_dates(date_key),
        nights INTEGER,
        num_guests INTEGER,
        base_price DECIMAL(10,2),
        total_price DECIMAL(10,2),
        booking_status VARCHAR(50),
        revenue DECIMAL(10,2),
        created_at TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS gold.agg_property_performance (
        property_id BIGINT,
        metric_date DATE,
        bookings_count INTEGER DEFAULT 0,
        revenue_total DECIMAL(10,2) DEFAULT 0,
        nights_booked INTEGER DEFAULT 0,
        occupancy_rate DECIMAL(5,2) DEFAULT 0,
        avg_nightly_rate DECIMAL(10,2),
        avg_rating DECIMAL(3,2),
        review_count INTEGER DEFAULT 0,
        cancellation_count INTEGER DEFAULT 0,
        PRIMARY KEY (property_id, metric_date)
    );
    
    CREATE TABLE IF NOT EXISTS gold.agg_city_metrics (
        city VARCHAR(100),
        metric_date DATE,
        active_properties INTEGER,
        total_bookings INTEGER,
        total_revenue DECIMAL(15,2),
        avg_occupancy_rate DECIMAL(5,2),
        avg_nightly_rate DECIMAL(10,2),
        PRIMARY KEY (city, metric_date)
    );
    
    CREATE TABLE IF NOT EXISTS gold.pricing_recommendations (
        property_id BIGINT,
        recommendation_date DATE,
        current_price DECIMAL(10,2),
        recommended_price DECIMAL(10,2),
        price_change_pct DECIMAL(5,2),
        reason TEXT,
        confidence_score DECIMAL(3,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (property_id, recommendation_date)
    );
    
    -- ========================================
    -- METADATA LAYER
    -- ========================================
    
    CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
        run_id SERIAL PRIMARY KEY,
        pipeline_name VARCHAR(255) NOT NULL,
        run_date TIMESTAMP NOT NULL,
        status VARCHAR(50) NOT NULL,
        records_processed INTEGER,
        records_failed INTEGER,
        error_message TEXT,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS metadata.data_quality_checks (
        check_id SERIAL PRIMARY KEY,
        table_name VARCHAR(255) NOT NULL,
        check_type VARCHAR(100) NOT NULL,
        check_status VARCHAR(50) NOT NULL,
        records_checked INTEGER,
        records_failed INTEGER,
        failure_rate DECIMAL(5,2),
        error_details JSONB,
        checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Create indexes for performance
    CREATE INDEX idx_pipeline_runs_name_date ON metadata.pipeline_runs(pipeline_name, run_date);
    CREATE INDEX idx_fact_bookings_property ON gold.fact_bookings(property_key);
    CREATE INDEX idx_fact_bookings_dates ON gold.fact_bookings(check_in_date_key, check_out_date_key);

    -- Insert sample date dimension data (next 2 years)
    INSERT INTO gold.dim_dates (date_key, calendar_date, day_of_week, day_name, week_of_year, month, month_name, quarter, year, is_weekend, is_holiday, season)
    SELECT 
        TO_CHAR(d, 'YYYYMMDD')::INTEGER as date_key,
        d::DATE as calendar_date,
        EXTRACT(DOW FROM d)::INTEGER as day_of_week,
        TO_CHAR(d, 'Day') as day_name,
        EXTRACT(WEEK FROM d)::INTEGER as week_of_year,
        EXTRACT(MONTH FROM d)::INTEGER as month,
        TO_CHAR(d, 'Month') as month_name,
        EXTRACT(QUARTER FROM d)::INTEGER as quarter,
        EXTRACT(YEAR FROM d)::INTEGER as year,
        EXTRACT(DOW FROM d) IN (0, 6) as is_weekend,
        FALSE as is_holiday,
        CASE 
            WHEN EXTRACT(MONTH FROM d) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM d) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM d) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END as season
    FROM generate_series('2024-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) d
    ON CONFLICT (date_key) DO NOTHING;

    ECHO "✅ Database initialization complete!"
EOSQL