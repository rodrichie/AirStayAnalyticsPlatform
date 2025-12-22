-- Create table for search trends
CREATE TABLE IF NOT EXISTS silver.search_trends_cities (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    city VARCHAR(100) NOT NULL,
    search_count INTEGER NOT NULL,
    unique_users INTEGER NOT NULL,
    avg_results NUMERIC(10, 2),
    avg_duration_ms NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (window_start, city)
);

CREATE INDEX idx_search_trends_city_time ON silver.search_trends_cities(city, window_start DESC);
CREATE INDEX idx_search_trends_time ON silver.search_trends_cities(window_start DESC);

-- Create materialized view for trending destinations
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.trending_destinations AS
SELECT 
    city,
    DATE(window_start) as trend_date,
    SUM(search_count) as daily_searches,
    AVG(unique_users) as avg_hourly_users,
    AVG(avg_results) as avg_results_shown,
    -- Calculate trend (compare to previous day)
    LAG(SUM(search_count)) OVER (PARTITION BY city ORDER BY DATE(window_start)) as prev_day_searches,
    CASE 
        WHEN LAG(SUM(search_count)) OVER (PARTITION BY city ORDER BY DATE(window_start)) > 0 
        THEN ((SUM(search_count)::float / LAG(SUM(search_count)) OVER (PARTITION BY city ORDER BY DATE(window_start))) - 1) * 100
        ELSE NULL
    END as growth_percentage
FROM silver.search_trends_cities
WHERE window_start >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY city, DATE(window_start)
ORDER BY daily_searches DESC;

-- Create index for faster refresh
CREATE INDEX idx_trending_dest_date ON gold.trending_destinations(trend_date DESC);

-- Refresh function (call from cron or Airflow)
CREATE OR REPLACE FUNCTION refresh_trending_destinations()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY gold.trending_destinations;
END;
$$ LANGUAGE plpgsql;