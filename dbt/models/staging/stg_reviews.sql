{{
    config(
        materialized='view'
    )
}}

WITH source_reviews AS (
    SELECT 
        review_id,
        property_id,
        booking_id,
        guest_id,
        rating,
        cleanliness_rating,
        accuracy_rating,
        communication_rating,
        location_rating,
        value_rating,
        review_text,
        review_language,
        sentiment_score,
        sentiment_label,
        host_response,
        host_response_time_hours,
        created_at
    FROM {{ source('silver', 'reviews') }}
)

SELECT 
    review_id,
    property_id,
    booking_id,
    guest_id,
    rating AS overall_rating,
    COALESCE(cleanliness_rating, rating) AS cleanliness_rating,
    COALESCE(accuracy_rating, rating) AS accuracy_rating,
    COALESCE(communication_rating, rating) AS communication_rating,
    COALESCE(location_rating, rating) AS location_rating,
    COALESCE(value_rating, rating) AS value_rating,
    review_text,
    review_language,
    COALESCE(sentiment_score, 0) AS sentiment_score,
    sentiment_label,
    host_response,
    host_response_time_hours,
    created_at AS review_date,
    -- Derived fields
    CASE 
        WHEN host_response IS NOT NULL THEN 1
        ELSE 0
    END AS has_host_response,
    
    (cleanliness_rating + accuracy_rating + communication_rating + 
     location_rating + value_rating) / 5.0 AS avg_aspect_rating,
    
    LENGTH(review_text) AS review_length,
    
    CASE 
        WHEN rating >= 4 THEN 'Positive'
        WHEN rating >= 3 THEN 'Neutral'
        ELSE 'Negative'
    END AS rating_sentiment

FROM source_reviews
WHERE rating IS NOT NULL