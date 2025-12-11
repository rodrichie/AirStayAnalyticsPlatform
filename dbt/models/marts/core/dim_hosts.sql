{{
    config(
        materialized='table',
        unique_key='host_key'
    )
}}

WITH hosts AS (
    SELECT 
        host_id,
        host_name,
        host_since,
        host_location,
        host_response_rate,
        host_acceptance_rate,
        is_superhost,
        total_listings,
        verified_email,
        verified_phone,
        created_at,
        updated_at
    FROM {{ source('silver', 'hosts') }}
),

-- Aggregate host stats
host_metrics AS (
    SELECT 
        p.host_id,
        COUNT(DISTINCT p.property_id) AS active_properties,
        AVG(p.property_rating) AS avg_property_rating,
        SUM(p.review_count) AS total_reviews
    FROM {{ ref('stg_properties') }} p
    GROUP BY p.host_id
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['h.host_id']) }} AS host_key,
    h.host_id,
    h.host_name,
    h.host_since,
    h.host_location,
    COALESCE(h.host_response_rate, 0) AS response_rate,
    COALESCE(h.host_acceptance_rate, 0) AS acceptance_rate,
    h.is_superhost,
    COALESCE(hm.active_properties, 0) AS active_properties,
    COALESCE(hm.avg_property_rating, 0) AS avg_property_rating,
    COALESCE(hm.total_reviews, 0) AS total_reviews,
    h.verified_email,
    h.verified_phone,
    -- Performance tier
    CASE 
        WHEN h.is_superhost AND hm.avg_property_rating >= 4.8 THEN 'Elite'
        WHEN h.is_superhost THEN 'Superhost'
        WHEN hm.avg_property_rating >= 4.5 THEN 'Top Performer'
        WHEN hm.avg_property_rating >= 4.0 THEN 'Good'
        ELSE 'Standard'
    END AS performance_tier,
    h.created_at,
    h.updated_at
FROM hosts h
LEFT JOIN host_metrics hm ON h.host_id = hm.host_id