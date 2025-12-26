#!/bin/bash
# Production deployment script

set -e

echo "🚀 AirStay Analytics Platform - Deployment"
echo "=========================================="

# Check environment
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found"
    echo "   Copy .env.example to .env and configure"
    exit 1
fi

# Load environment
source .env

echo ""
echo "📋 Pre-deployment Checklist:"
echo "  [1/5] Checking Docker..."
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not installed"
    exit 1
fi
echo "  ✅ Docker OK"

echo "  [2/5] Checking Docker Compose..."
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose not installed"
    exit 1
fi
echo "  ✅ Docker Compose OK"

echo "  [3/5] Building images..."
docker-compose -f docker-compose.prod.yml build --no-cache
echo "  ✅ Images built"

echo "  [4/5] Starting services..."
docker-compose -f docker-compose.prod.yml up -d
echo "  ✅ Services started"

echo "  [5/5] Waiting for services to be healthy..."
sleep 10

# Check service health
echo ""
echo "🏥 Health Checks:"

# Database
if docker-compose -f docker-compose.prod.yml exec -T postgres pg_isready -U $DB_USER > /dev/null 2>&1; then
    echo "  ✅ PostgreSQL"
else
    echo "  ❌ PostgreSQL not ready"
fi

# Redis
if docker-compose -f docker-compose.prod.yml exec -T redis redis-cli ping > /dev/null 2>&1; then
    echo "  ✅ Redis"
else
    echo "  ❌ Redis not ready"
fi

# API
if curl -s http://localhost/health > /dev/null 2>&1; then
    echo "  ✅ API"
else
    echo "  ❌ API not ready"
fi

echo ""
echo "✅ Deployment Complete!"
echo ""
echo "📊 Access Points:"
echo "   API: http://localhost/api/v1/docs"
echo "   Dashboard: http://localhost:8501"
echo "   Airflow: http://localhost:8080"
echo "   Grafana: http://localhost:3000"
echo ""
echo "📝 Next Steps:"
echo "   1. Initialize database: make init-db"
echo "   2. Load sample data: make load-data"
echo "   3. Train ML models: make train-models"
echo ""