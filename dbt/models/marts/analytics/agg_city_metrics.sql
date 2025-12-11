{{
    config(
        materialized='incremental',
        unique_key=['city', 'metric_date']
    )
}}

WITH city_bookings AS (
    SELECT 
        p.city,
        DATE(b.check_in_date) AS metric_date,
        COUNT(DISTINCT b.booking_id) AS total_bookings,
        SUM(b.revenue) AS total_revenue,
        AVG(b.avg_nightly_rate) AS avg_nightly_rate,
        COUNT(DISTINCT b.property_key) AS properties_with_bookings
    FROM {{ ref('fact_bookings') }} b
    INNER JOIN {{ ref('dim_properties') }} p ON b.property_key = p.property_key
    {% if is_incremental() %}
    WHERE DATE(b.check_in_date) >= (SELECT MAX(metric_date) FROM {{ this }})
    {% endif %}
    GROUP BY p.city, DATE(b.check_in_date)
),

city_properties AS (
    SELECT 
        city,
        COUNT(DISTINCT property_id) AS active_properties,
        AVG(base_price) AS avg_base_price
    FROM {{ ref('dim_properties') }}
    WHERE is_current = TRUE
    GROUP BY city
)

SELECT 
    b.city,
    b.metric_date,
    p.active_properties,
    b.total_bookings,
    b.total_revenue,
    b.avg_nightly_rate,
    -- Occupancy estimation
    CASE 
        WHEN p.active_properties > 0 
        THEN (b.properties_with_bookings::float / p.active_properties * 100)
        ELSE 0
    END AS avg_occupancy_rate,
    p.avg_base_price,
    CURRENT_TIMESTAMP AS updated_at
FROM city_bookings b
INNER JOIN city_properties p ON b.city = p.city