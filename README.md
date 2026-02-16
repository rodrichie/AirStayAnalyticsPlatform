# AirStay Analytics Platform

A production-grade end-to-end data engineering and ML platform for vacation rental analytics, inspired by Airbnb's architecture.

## Project Overview

This project demonstrates advanced data engineering skills through a complete lakehouse implementation with:

- **Real-time streaming** (Kafka + Spark)
- **Batch processing** (Airflow + dbt)
- **ML pipelines** (XGBoost, LightFM, NLP)
- **API serving** (FastAPI + Redis)
- **Analytics dashboard** (Streamlit)

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   MinIO     │────▶│    Kafka     │────▶│   Spark     │
│  (Images)   │     │  (Events)    │     │ Streaming   │
└─────────────┘     └──────────────┘     └─────────────┘
                                                 │
                                                 ▼
┌─────────────────────────────────────────────────────────┐
│            PostgreSQL + PostGIS                          │
│  ┌────────┐  ┌────────┐  ┌────────┐                    │
│  │ Bronze │─▶│ Silver │─▶│  Gold  │                    │
│  │  (Raw) │  │(Cleaned)│ │(Analytics)│                 │
│  └────────┘  └────────┘  └────────┘                    │
└─────────────────────────────────────────────────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
    ┌────────┐          ┌────────┐          ┌────────┐
    │ Airflow│          │  dbt   │          │ FastAPI│
    │  (ETL) │          │(Transform)        │ + Redis│
    └────────┘          └────────┘          └────────┘
                                                 │
                                                 ▼
                                           ┌──────────┐
                                           │Streamlit │
                                           │Dashboard │
                                           └──────────┘
```

## Quick Start

### Prerequisites

- Docker & Docker Compose
- 16GB RAM minimum
- 50GB disk space

### Installation

1. **Clone repository**

```bash
git clone https://github.com/rodrichie/airstay-analytics.git
cd airstay-analytics
```

1. **Configure environment**

```bash
cp .env.example .env
# Edit .env with your configuration
```

1. **Deploy**

```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh
```

1. **Initialize data**

```bash
make init-db
make load-sample-data
```

1. **Access applications**

| Service | URL | Credentials |
|---------|-----|-------------|
| **API - Swagger UI** | <http://localhost:8010/api/v1/docs> | - |
| **API - ReDoc** | <http://localhost:8010/api/v1/redoc> | - |
| Streamlit Dashboard | <http://localhost:8512> | - |
| Airflow | <http://localhost:8085> | admin / admin |
| Grafana | <http://localhost:3001> | admin / admin |
| Prometheus | <http://localhost:9092> | - |
| MinIO Console | <http://localhost:9011> | minioadmin / minioadmin |

## Project Structure

```
airstay-analytics/
├── airflow/
│   ├── dags/                    # Airflow DAGs
│   └── models/                  # Trained ML models
├── api/
│   ├── routers/                 # FastAPI endpoints
│   ├── schemas.py               # Pydantic models
│   └── main.py                  # API application
├── dashboard/
│   └── app.py                   # Streamlit dashboard
├── dbt/
│   ├── models/                  # dbt transformations
│   └── dbt_project.yml
├── ml/
│   ├── features/                # Feature engineering
│   └── models/                  # ML model implementations
├── streaming/
│   ├── producers/               # Kafka producers
│   ├── spark/                   # Spark streaming jobs
│   └── schemas/                 # Event schemas
├── scripts/
│   ├── init-postgres.sh
│   ├── generate_sample_data.py
│   └── deploy.sh
├── tests/
│   └── load_testing/            # Locust load tests
├── docker-compose.yml
├── docker-compose.prod.yml
└── README.md
```

## Key Features

### Data Engineering

- **3-layer lakehouse**: Bronze (raw) → Silver (cleaned) → Gold (analytics)
- **Real-time streaming**: 1000+ events/sec via Kafka
- **Batch orchestration**: 15+ Airflow DAGs
- **Data quality**: Automated validation & monitoring

### Machine Learning

- **Dynamic Pricing**: XGBoost model (MAPE <10%)
- **Recommendations**: Hybrid collaborative filtering (Precision@10 >0.25)
- **Sentiment Analysis**: Multi-language NLP (20+ languages)
- **Anomaly Detection**: Isolation Forest for fraud detection
- **Demand Forecasting**: Time series with seasonality

### APIs & Serving

- **20+ REST endpoints** with FastAPI
- **Redis caching**: <10ms response times
- **Rate limiting**: 100 req/min per IP
- **Load balancing**: Nginx with 2+ API instances
- **95th percentile latency**: <200ms

### Analytics

- **Interactive dashboard**: Streamlit with real-time updates
- **Performance metrics**: Property/city/platform analytics
- **Monitoring**: Prometheus + Grafana
- **A/B testing**: Built-in experimentation framework

## Performance Benchmarks

| Metric | Achievement |
|--------|-------------|
| API Throughput | 1,200 RPS |
| API Latency (p95) | 185ms |
| Cache Hit Rate | 87% |
| Spark Processing | 50K events/sec |
| ML Prediction Latency | <50ms |
| Database Query Time | <100ms avg |

## Testing

```bash
# Unit tests
make test

# Load testing
cd tests/load_testing
locust -f locustfile.py --host http://localhost

# Integration tests
make test-integration
```

## API Documentation

The API provides interactive documentation via OpenAPI/Swagger:

- **Swagger UI**: <http://localhost:8010/api/v1/docs> -- Interactive API explorer with "Try it out" functionality
- **ReDoc**: <http://localhost:8010/api/v1/redoc> -- Clean, readable API reference
- **OpenAPI JSON**: <http://localhost:8010/api/v1/openapi.json> -- Machine-readable specification

The documentation covers all 16+ endpoints across Properties, Bookings, Pricing, Recommendations, and Analytics with request/response schemas and caching details.

## Documentation

- [Architecture Overview](docs/ARCHITECTURE.md)
- [ML Models Guide](docs/ML_MODELS.md)
- [Performance Optimization](docs/PERFORMANCE_OPTIMIZATION.md)
- [Deployment Guide](docs/DEPLOYMENT.md)

## Tech Stack

**Data Processing**

- Apache Airflow 2.8
- Apache Spark 3.5
- Apache Kafka 3.6
- dbt 1.7

**Databases**

- PostgreSQL 15 + PostGIS
- Redis 7

**ML/Analytics**

- XGBoost
- LightFM
- HuggingFace Transformers
- scikit-learn
- Plotly

**APIs/Serving**

- FastAPI 0.104
- Streamlit 1.28
- Nginx

**Infrastructure**

- Docker & Docker Compose
- Prometheus + Grafana

## Use Cases

This platform demonstrates skills for:

- **Data Engineer**: ETL pipelines, lakehouse architecture
- **ML Engineer**: End-to-end ML pipelines, model serving
- **Analytics Engineer**: dbt transformations, metrics
- **Backend Engineer**: API design, caching, optimization

## Sample Queries

### Get Property Recommendations

```bash
curl "http://localhost/api/v1/recommendations/user/1001?n_recommendations=10"
```

### Search Properties

```bash
curl "http://localhost/api/v1/properties/search?city=New%20York&num_guests=2&max_price=200"
```

### Get Analytics Dashboard

```bash
curl "http://localhost/api/v1/analytics/dashboard/summary?days=30"
```

## Contributing

This is a portfolio project, but feedback is welcome!

## License

MIT License - see LICENSE file

## Author

**Rodrick Nabasa**

- LinkedIn: linkedin.com/in/rodrick-nabasa-235151283
- GitHub: github.com/rodrichie
- Email: nabasarodrick@gmail.com

---

