{{ config(
    materialized='incremental',
    unique_key=['property_id', 'metric_date'],
    schema='gold'
) }}

SELECT
    b.property_id,
    DATE(b.created_at) as metric_date,
    COUNT(*) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')) as bookings_count,
    COALESCE(SUM(b.total_price) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')), 0) as revenue_total,
    COALESCE(SUM(b.nights) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')), 0) as nights_booked,
    ROUND(
        COALESCE(SUM(b.nights) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')), 0)::DECIMAL / 30 * 100,
        2
    ) as occupancy_rate,
    ROUND(AVG(p.base_price), 2) as avg_nightly_rate,
    ROUND(AVG(p.property_rating), 2) as avg_rating,
    COUNT(DISTINCT r.review_id) as review_count,
    COUNT(*) FILTER (WHERE b.booking_status = 'canceled') as cancellation_count
FROM silver.bookings b
JOIN silver.properties p ON b.property_id = p.property_id
LEFT JOIN silver.reviews r ON r.property_id = b.property_id
{% if is_incremental() %}
WHERE DATE(b.created_at) > (SELECT MAX(metric_date) FROM {{ this }})
{% endif %}
GROUP BY b.property_id, DATE(b.created_at)
