-- Anomaly detections table
CREATE TABLE IF NOT EXISTS metadata.anomaly_detections (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,  -- 'booking', 'review', 'user'
    entity_id INTEGER NOT NULL,
    anomaly_score NUMERIC(10, 4),
    risk_level VARCHAR(20),  -- 'low', 'medium', 'high'
    flags TEXT,  -- Comma-separated flags
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMP,
    reviewed_by VARCHAR(100),
    status VARCHAR(50) DEFAULT 'pending',  -- 'pending', 'approved', 'blocked', 'false_positive'
    notes TEXT,
    UNIQUE (entity_type, entity_id, detected_at::date)
);

CREATE INDEX idx_anomaly_entity ON metadata.anomaly_detections(entity_type, entity_id);
CREATE INDEX idx_anomaly_status ON metadata.anomaly_detections(status, detected_at DESC);
CREATE INDEX idx_anomaly_risk ON metadata.anomaly_detections(risk_level, detected_at DESC);

-- ML model metrics table
CREATE TABLE IF NOT EXISTS metadata.ml_model_metrics (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50),
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(10, 4),
    trained_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

CREATE INDEX idx_ml_metrics_model ON metadata.ml_model_metrics(model_name, trained_at DESC);