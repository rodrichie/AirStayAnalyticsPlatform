-- Model predictions log
CREATE TABLE IF NOT EXISTS metadata.model_predictions (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50),
    entity_id INTEGER NOT NULL,
    prediction NUMERIC(12, 4),
    features JSONB,
    actual_value NUMERIC(12, 4),
    metadata JSONB,
    predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_model_predictions_model ON metadata.model_predictions(model_name, predicted_at DESC);
CREATE INDEX idx_model_predictions_entity ON metadata.model_predictions(entity_id);

-- A/B tests
CREATE TABLE IF NOT EXISTS metadata.ab_tests (
    test_id SERIAL PRIMARY KEY,
    test_name VARCHAR(100) UNIQUE NOT NULL,
    model_a VARCHAR(100) NOT NULL,
    model_b VARCHAR(100) NOT NULL,
    traffic_split NUMERIC(3, 2) DEFAULT 0.5,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    status VARCHAR(50) DEFAULT 'active',
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- A/B test assignments
CREATE TABLE IF NOT EXISTS metadata.ab_test_assignments (
    test_id INTEGER REFERENCES metadata.ab_tests(test_id),
    entity_id INTEGER NOT NULL,
    variant CHAR(1) NOT NULL,  -- 'a' or 'b'
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (test_id, entity_id)
);

CREATE INDEX idx_ab_assignments_variant ON metadata.ab_test_assignments(test_id, variant);