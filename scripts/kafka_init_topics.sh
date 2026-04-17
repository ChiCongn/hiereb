#!/bin/bash
# =============================================================================
# scripts/kafka-init-topics.sh
# 
# Purpose: Create required Kafka topics for HierEB system on startup.
#          Run as part of kafka-init service in docker-compose.yml
# 
# Topics created:
#   - hiereb.sensor_data     (partitioned 4)  → high volume from simulator
#   - hiereb.predictions     (partition 1)    → batch predictions from ML service
#   - hiereb.thresholds      (partition 1)    → delta thresholds from allocator
#   - hiereb.variance        (partitioned 4)  → variance updates from simulator
# 
# Conventions followed:
#   - Uses KRaft-mode Kafka 3.7
#   - Idempotent (--if-not-exists)
#   - Clear logging with emojis for easy reading in docker logs
# =============================================================================

set -euo pipefail

# Configuration
KAFKA_CONTAINER="kafka"
KAFKA_BIN="/opt/kafka/bin/kafka-topics.sh"
BOOTSTRAP_SERVER="localhost:9092"
MAX_WAIT=30

echo "=== HierEB Kafka Topic Initialization ==="
echo "Waiting for Kafka broker to be ready..."

# Wait for Kafka to be ready (healthcheck-style)
for i in $(seq 1 $MAX_WAIT); do
    if docker exec "$KAFKA_CONTAINER" "$KAFKA_BIN" --list --bootstrap-server "$BOOTSTRAP_SERVER" >/dev/null 2>&1; then
        echo "✅ Kafka broker is ready!"
        break
    fi
    
    echo "⏳ Waiting for Kafka... ($i/$MAX_WAIT)"
    sleep 2
done

# Final check
if ! docker exec "$KAFKA_CONTAINER" "$KAFKA_BIN" --list --bootstrap-server "$BOOTSTRAP_SERVER" >/dev/null 2>&1; then
    echo "❌ ERROR: Kafka is not ready after $MAX_WAIT attempts. Exiting."
    exit 1
fi

echo "=== Creating/Verifying topics ==="

create_topic() {
    local topic_name="$1"
    local partitions="$2"
    
    echo "→ Creating topic: $topic_name (partitions: $partitions)"
    
    docker exec "$KAFKA_CONTAINER" "$KAFKA_BIN" \
        --create \
        --if-not-exists \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --topic "$topic_name" \
        --partitions "$partitions" \
        --replication-factor 1
    
    echo "   ✅ Topic '$topic_name' ready"
}

# Create topics with appropriate partitioning strategy
create_topic "hiereb.sensor_data" 4     # High volume → more partitions
create_topic "hiereb.predictions" 1     # Low volume batch messages
create_topic "hiereb.thresholds" 1      # Low volume control messages
create_topic "hiereb.variance" 4        # Medium volume variance updates

echo ""
echo "=== All topics created successfully! ==="
echo "Current topics in Kafka:"
docker exec "$KAFKA_CONTAINER" "$KAFKA_BIN" --list --bootstrap-server "$BOOTSTRAP_SERVER"

echo ""
echo "HierEB Kafka initialization completed."