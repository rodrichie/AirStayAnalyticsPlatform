{{ config(
    materialized='incremental',
    unique_key=['city', 'metric_date'],
    schema='gold'
) }}

SELECT
    p.location_city as city,
    DATE(b.created_at) as metric_date,
    COUNT(DISTINCT p.property_id) as active_properties,
    COUNT(*) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')) as total_bookings,
    COALESCE(SUM(b.total_price) FILTER (WHERE b.booking_status IN ('confirmed', 'completed')), 0) as total_revenue,
    ROUND(AVG(COALESCE(b.nights, 0))::DECIMAL / 30 * 100, 2) as avg_occupancy_rate,
    ROUND(AVG(p.base_price), 2) as avg_nightly_rate
FROM silver.bookings b
JOIN silver.properties p ON b.property_id = p.property_id
{% if is_incremental() %}
WHERE DATE(b.created_at) > (SELECT MAX(metric_date) FROM {{ this }})
{% endif %}
GROUP BY p.location_city, DATE(b.created_at)
