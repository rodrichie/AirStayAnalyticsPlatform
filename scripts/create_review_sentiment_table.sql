-- Review sentiment aggregates table
CREATE TABLE IF NOT EXISTS silver.review_sentiment_aggregates (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    property_id INTEGER NOT NULL,
    review_count INTEGER NOT NULL,
    avg_rating NUMERIC(3, 2),
    avg_sentiment NUMERIC(5, 3),
    positive_count INTEGER,
    neutral_count INTEGER,
    negative_count INTEGER,
    avg_cleanliness NUMERIC(3, 2),
    avg_accuracy NUMERIC(3, 2),
    avg_communication NUMERIC(3, 2),
    avg_location NUMERIC(3, 2),
    avg_value NUMERIC(3, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (window_start, property_id)
);

CREATE INDEX idx_review_sentiment_property ON silver.review_sentiment_aggregates(property_id, window_start DESC);
CREATE INDEX idx_review_sentiment_rating ON silver.review_sentiment_aggregates(avg_rating DESC);