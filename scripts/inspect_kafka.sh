#!/usr/bin/env bash
# Inspect Kafka broker, topics, offsets, consumer groups, and recent logs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/inspect_common.sh"

section "Kafka Status"
try_cmd "${COMPOSE[@]}" ps kafka kafka-init

section "Topic List"
kafka_tool kafka-topics.sh --list

section "Topic Details"
for topic in hiereb.sensor_data hiereb.predictions hiereb.thresholds hiereb.variance; do
  kafka_tool kafka-topics.sh --describe --topic "$topic"
done

section "Topic End Offsets"
for topic in hiereb.sensor_data hiereb.predictions hiereb.thresholds hiereb.variance; do
  echo "Topic: $topic"
  kafka_offsets "$topic"
done

section "Consumer Groups"
kafka_tool kafka-consumer-groups.sh --list

section "Aggregator Consumer Lag"
kafka_tool kafka-consumer-groups.sh --describe --group hiereb-aggregator

section "ML Consumer Lag"
kafka_tool kafka-consumer-groups.sh --describe --group hiereb-ml

section "Kafka Logs"
service_logs kafka

section "Kafka Init Logs"
service_logs kafka-init
