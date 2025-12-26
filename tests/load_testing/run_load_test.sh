#!/bin/bash
# Run load tests with different scenarios

echo "🧪 AirStay Load Testing Suite"
echo "================================"

# Test 1: Light load (10 users)
echo ""
echo "📊 Test 1: Light Load (10 concurrent users)"
locust -f locustfile.py \
    --host http://localhost:8000 \
    --users 10 \
    --spawn-rate 2 \
    --run-time 2m \
    --headless \
    --only-summary

# Test 2: Medium load (50 users)
echo ""
echo "📊 Test 2: Medium Load (50 concurrent users)"
locust -f locustfile.py \
    --host http://localhost:8000 \
    --users 50 \
    --spawn-rate 5 \
    --run-time 5m \
    --headless \
    --only-summary

# Test 3: Heavy load (200 users)
echo ""
echo "📊 Test 3: Heavy Load (200 concurrent users)"
locust -f locustfile.py \
    --host http://localhost:8000 \
    --users 200 \
    --spawn-rate 10 \
    --run-time 5m \
    --headless \
    --only-summary

# Test 4: Spike test (rapid increase)
echo ""
echo "📊 Test 4: Spike Test (0 to 500 users in 30s)"
locust -f locustfile.py \
    --host http://localhost:8000 \
    --users 500 \
    --spawn-rate 16 \
    --run-time 3m \
    --headless \
    --only-summary

echo ""
echo "✅ All load tests completed!"