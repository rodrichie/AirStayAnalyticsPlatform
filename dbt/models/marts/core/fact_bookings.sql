{{
    config(
        materialized='table',
        unique_key='booking_key'
    )
}}

WITH bookings AS (
    SELECT * FROM {{ ref('stg_bookings') }}
),

properties AS (
    SELECT 
        property_key,
        property_id,
        host_id
    FROM {{ ref('dim_properties') }}
    WHERE is_current = TRUE
),

hosts AS (
    SELECT 
        host_key,
        host_id
    FROM {{ ref('dim_hosts') }}
),

dates AS (
    SELECT 
        date_key,
        calendar_date
    FROM {{ ref('dim_dates') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['b.booking_id']) }} AS booking_key,
    b.booking_id,
    p.property_key,
    h.host_key,
    d_checkin.date_key AS check_in_date_key,
    d_checkout.date_key AS check_out_date_key,
    d_booking.date_key AS booking_date_key,
    b.guest_id,
    b.nights,
    b.num_guests,
    b.base_price,
    b.cleaning_fee,
    b.service_fee,
    b.total_price,
    b.avg_nightly_rate,
    b.revenue,
    b.booking_status,
    b.booking_channel,
    b.is_canceled,
    b.cancellation_reason,
    b.hours_to_confirm,
    b.lead_time_days,
    b.booking_date,
    b.check_in_date,
    b.check_out_date,
    b.confirmed_at,
    b.canceled_at
FROM bookings b
INNER JOIN properties p ON b.property_id = p.property_id
INNER JOIN hosts h ON p.host_id = h.host_id
LEFT JOIN dates d_checkin ON b.check_in_date = d_checkin.calendar_date
LEFT JOIN dates d_checkout ON b.check_out_date = d_checkout.calendar_date
LEFT JOIN dates d_booking ON b.booking_date::date = d_booking.calendar_date