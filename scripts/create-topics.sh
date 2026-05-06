#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER=${KAFKA_CONTAINER:-kafka}
BOOTSTRAP_SERVER=${BOOTSTRAP_SERVER:-localhost:9092}
PARTITIONS=${PARTITIONS:-3}
REPLICATION=${REPLICATION:-1}

KAFKA_TOPICS_CMD="/opt/kafka/bin/kafka-topics.sh"

TOPICS=("raw-events" "failed-events")

echo "Waiting for Kafka to be ready..."

until docker exec "$KAFKA_CONTAINER" sh -c "$KAFKA_TOPICS_CMD --bootstrap-server $BOOTSTRAP_SERVER --list" >/dev/null 2>&1; do
  sleep 2
done

for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC"
  docker exec "$KAFKA_CONTAINER" sh -c "$KAFKA_TOPICS_CMD \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --create \
    --if-not-exists \
    --topic $TOPIC \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION"
  echo "Topic '$TOPIC' ready (partitions=$PARTITIONS, replication=$REPLICATION)"
done
