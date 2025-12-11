{{
    config(
        materialized='view'
    )
}}

WITH source_properties AS (
    SELECT 
        property_id,
        listing_id,
        host_id,
        property_type,
        title,
        description,
        bedrooms,
        bathrooms,
        beds,
        max_guests,
        base_price,
        cleaning_fee,
        amenities,
        location_city,
        location_state,
        location_country,
        neighborhood,
        latitude,
        longitude,
        instant_bookable,
        minimum_nights,
        maximum_nights,
        cancellation_policy,
        response_time_minutes,
        host_is_superhost,
        property_rating,
        review_count,
        quality_score,
        is_active,
        created_at,
        updated_at
    FROM {{ source('silver', 'properties') }}
    WHERE is_active = TRUE
)

SELECT 
    property_id,
    listing_id,
    host_id,
    property_type,
    title,
    description,
    bedrooms,
    bathrooms,
    beds,
    max_guests,
    base_price,
    cleaning_fee,
    COALESCE(array_length(amenities, 1), 0) AS amenities_count,
    location_city AS city,
    location_state AS state,
    location_country AS country,
    neighborhood,
    latitude,
    longitude,
    instant_bookable AS is_instant_bookable,
    minimum_nights,
    maximum_nights,
    cancellation_policy,
    response_time_minutes,
    host_is_superhost AS is_superhost,
    COALESCE(property_rating, 0) AS rating,
    COALESCE(review_count, 0) AS review_count,
    COALESCE(quality_score, 0) AS quality_score,
    created_at,
    updated_at,
    -- Derived fields
    CASE 
        WHEN property_rating >= 4.5 THEN 'Excellent'
        WHEN property_rating >= 4.0 THEN 'Very Good'
        WHEN property_rating >= 3.5 THEN 'Good'
        WHEN property_rating >= 3.0 THEN 'Fair'
        ELSE 'Poor'
    END AS rating_category,
    
    CASE 
        WHEN base_price < 100 THEN 'Budget'
        WHEN base_price < 200 THEN 'Mid-Range'
        WHEN base_price < 400 THEN 'Upscale'
        ELSE 'Luxury'
    END AS price_category,
    
    base_price + COALESCE(cleaning_fee, 0) AS total_base_cost

FROM source_properties