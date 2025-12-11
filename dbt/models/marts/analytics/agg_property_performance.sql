{{
    config(
        materialized='incremental',
        unique_key=['property_id', 'metric_date']
    )
}}

WITH daily_bookings AS (
    SELECT 
        p.property_id,
        DATE(b.check_in_date) AS metric_date,
        COUNT(DISTINCT b.booking_id) AS bookings_count,
        SUM(b.revenue) AS revenue_total,
        SUM(b.nights) AS nights_booked,
        AVG(b.avg_nightly_rate) AS avg_nightly_rate,
        COUNT(DISTINCT CASE WHEN b.is_canceled = 1 THEN b.booking_id END) AS cancellation_count
    FROM {{ ref('fact_bookings') }} b
    INNER JOIN {{ ref('dim_properties') }} p ON b.property_key = p.property_key
    {% if is_incremental() %}
    WHERE DATE(b.check_in_date) >= (SELECT MAX(metric_date) FROM {{ this }})
    {% endif %}
    GROUP BY p.property_id, DATE(b.check_in_date)
),

daily_reviews AS (
    SELECT 
        property_id,
        DATE(review_date) AS metric_date,
        AVG(overall_rating) AS avg_rating,
        COUNT(*) AS review_count
    FROM {{ ref('stg_reviews') }}
    {% if is_incremental() %}
    WHERE DATE(review_date) >= (SELECT MAX(metric_date) FROM {{ this }})
    {% endif %}
    GROUP BY property_id, DATE(review_date)
),

calendar_availability AS (
    SELECT 
        property_id,
        calendar_date AS metric_date,
        COUNT(*) AS total_days,
        COUNT(*) FILTER (WHERE is_available = FALSE) AS booked_days
    FROM {{ source('silver', 'availability_calendar') }}
    {% if is_incremental() %}
    WHERE calendar_date >= (SELECT MAX(metric_date) FROM {{ this }})
    {% endif %}
    GROUP BY property_id, calendar_date
)

SELECT 
    COALESCE(b.property_id, r.property_id, c.property_id) AS property_id,
    COALESCE(b.metric_date, r.metric_date, c.metric_date) AS metric_date,
    COALESCE(b.bookings_count, 0) AS bookings_count,
    COALESCE(b.revenue_total, 0) AS revenue_total,
    COALESCE(b.nights_booked, 0) AS nights_booked,
    COALESCE(c.booked_days::float / NULLIF(c.total_days, 0) * 100, 0) AS occupancy_rate,
    COALESCE(b.avg_nightly_rate, 0) AS avg_nightly_rate,
    COALESCE(r.avg_rating, 0) AS avg_rating,
    COALESCE(r.review_count, 0) AS review_count,
    COALESCE(b.cancellation_count, 0) AS cancellation_count,
    CURRENT_TIMESTAMP AS updated_at
FROM daily_bookings b
FULL OUTER JOIN daily_reviews r 
    ON b.property_id = r.property_id 
    AND b.metric_date = r.metric_date
FULL OUTER JOIN calendar_availability c
    ON COALESCE(b.property_id, r.property_id) = c.property_id
    AND COALESCE(b.metric_date, r.metric_date) = c.metric_date