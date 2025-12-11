{{
    config(
        materialized='view'
    )
}}

WITH source_bookings AS (
    SELECT 
        booking_id,
        property_id,
        guest_id,
        check_in_date,
        check_out_date,
        nights,
        num_guests,
        base_price,
        cleaning_fee,
        service_fee,
        total_price,
        booking_status,
        booking_channel,
        created_at,
        confirmed_at,
        canceled_at,
        cancellation_reason
    FROM {{ source('silver', 'bookings') }}
)

SELECT 
    booking_id,
    property_id,
    guest_id,
    check_in_date,
    check_out_date,
    nights,
    num_guests,
    base_price,
    COALESCE(cleaning_fee, 0) AS cleaning_fee,
    COALESCE(service_fee, 0) AS service_fee,
    total_price,
    booking_status,
    booking_channel,
    created_at AS booking_date,
    confirmed_at,
    canceled_at,
    cancellation_reason,
    -- Derived fields
    base_price / NULLIF(nights, 0) AS avg_nightly_rate,
    
    CASE 
        WHEN booking_status = 'completed' THEN total_price
        ELSE 0
    END AS revenue,
    
    CASE 
        WHEN booking_status = 'canceled' THEN 1
        ELSE 0
    END AS is_canceled,
    
    CASE 
        WHEN confirmed_at IS NOT NULL THEN 
            EXTRACT(EPOCH FROM (confirmed_at - created_at)) / 3600.0
        ELSE NULL
    END AS hours_to_confirm,
    
    -- Lead time (days between booking and check-in)
    check_in_date - created_at::date AS lead_time_days

FROM source_bookings
WHERE booking_status IN ('confirmed', 'completed', 'canceled')