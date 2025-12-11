#!/bin/bash 
# Create kafka topics for AirStay event streaming

set -e

KAFKA_BROKER="kafka:9092"

echo "Creating kafka topics for AirStay Analytics.."

# Wait for kafka to be ready
echo "Waiting for kafka to be ready..."
sleep 10 

# Create topics with appropriate partitions and replication
docker exec -it airstay-kafka kafka-topics.sh \
    --create \
    --bootstrap-server $KAFKA_BROKER \
    --topic booking-events \
    --partitions 3 \
    --replication-factor 1
    --if-not-exists

docker exec -it airstay-kafka kafka-topics.sh \
    --create \
    --bootstrap-server $KAFKA_BROKER \
    --topic review-events \
    --partitions 2 \ 
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --if-not-exists

docker exec -it airstay-kafka kafka-topics.sh \
    --create \
    --bootstrap-server $KAFKA_BROKER \
    --topic search-events \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --if-not-exists

docker exec -it airstay-kafka kafka-topics.sh \
    --create \
    --bootstrap-server $KAFKA_BROKER \
    --topic availability-updates \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --if-not-exists

docker exec -it airstay-kafka kafka-topics.sh \
    --create \
    --bootstrap-server $KAFKA_BROKER \
    --topic price-changes \
    --partitions 2 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --if-not-exists

docker exec -it airstay-kafka kafka-topics.sh \
    --create \
    --bootstrap-server $KAFKA_BROKER \
    --topic user-activity \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=259200000 \
    --if-not-exists

echo "✅ Kafka topics created successfully!"

# List all topics
echo ""
echo " Available topics:"
docker exec -it airstay-kafka kafka-topics.sh \
     --list \
     --bootstrap-server $KAFKA_BROKER

# Describe booking-events topic
echo ""
echo " Booking-events topic configuration:"    
docker exec -it airstay-kafka kafka-topics.sh \
     --describe \
     --topic booking-events \
     --bootstrap-server $KAFKA_BROKER     