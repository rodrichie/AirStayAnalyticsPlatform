{{
    config(
        materialized='table',
        unique_key='property_key'
    )
}}

WITH properties AS (
    SELECT * FROM {{ ref('stg_properties') }}
),

-- Generate surrogate key
properties_with_key AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['property_id', 'updated_at']) }} AS property_key,
        property_id,
        listing_id,
        host_id,
        property_type,
        title,
        city,
        state,
        country,
        neighborhood,
        bedrooms,
        bathrooms,
        beds,
        max_guests,
        latitude,
        longitude,
        amenities_count,
        is_instant_bookable,
        minimum_nights,
        maximum_nights,
        cancellation_policy,
        is_superhost,
        rating,
        rating_category,
        review_count,
        quality_score,
        price_category,
        base_price,
        cleaning_fee,
        total_base_cost,
        created_at AS created_date,
        updated_at AS scd_start_date,
        -- SCD Type 2 fields
        LEAD(updated_at) OVER (
            PARTITION BY property_id 
            ORDER BY updated_at
        ) AS scd_end_date,
        CASE 
            WHEN LEAD(updated_at) OVER (
                PARTITION BY property_id 
                ORDER BY updated_at
            ) IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM properties
)

SELECT 
    property_key,
    property_id,
    listing_id,
    host_id,
    property_type,
    title,
    city,
    state,
    country,
    neighborhood,
    bedrooms,
    bathrooms,
    beds,
    max_guests,
    latitude,
    longitude,
    amenities_count,
    is_instant_bookable,
    minimum_nights,
    maximum_nights,
    cancellation_policy,
    is_superhost,
    rating,
    rating_category,
    review_count,
    quality_score,
    price_category,
    base_price,
    cleaning_fee,
    total_base_cost,
    created_date,
    scd_start_date,
    COALESCE(scd_end_date, '9999-12-31'::date) AS scd_end_date,
    is_current
FROM properties_with_key