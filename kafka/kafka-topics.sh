#!/bin/bash
# kafka-topics.sh: Manage Kafka topics for CDC pipeline.
# Usage:
#   ./kafka-topics.sh create <topic_name> <partitions> <replication_factor>
#   ./kafka-topics.sh list
#   ./kafka-topics.sh describe <topic_name>
#
# This script is intended to be run inside the Docker Compose environment.
# It assumes the Kafka broker is accessible as 'kafka:9092'.

set -e

KAFKA_BROKER="kafka:9092"
KAFKA_TOPICS_CMD="kafka-topics"

function create_topic() {
  local topic=$1
  local partitions=$2
  local replication=$3
  echo "Creating topic '$topic' with $partitions partitions and replication factor $replication..."
  docker exec kafka bash -c "$KAFKA_TOPICS_CMD \
    --create \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor $replication \
    --partitions $partitions \
    --topic $topic"
}

function list_topics() {
  echo "Listing all Kafka topics..."
  docker exec kafka bash -c "$KAFKA_TOPICS_CMD --list --bootstrap-server $KAFKA_BROKER"
}

function describe_topic() {
  local topic=$1
  echo "Describing topic '$topic'..."
  docker exec kafka bash -c "$KAFKA_TOPICS_CMD \
    --describe \
    --bootstrap-server $KAFKA_BROKER \
    --topic $topic"
}

# Main script
case $1 in
  create)
    if [ $# -ne 4 ]; then
      echo "Usage: $0 create <topic_name> <partitions> <replication_factor>"
      exit 1
    fi
    create_topic $2 $3 $4
    ;;
  list)
    list_topics
    ;;
  describe)
    if [ $# -ne 2 ]; then
      echo "Usage: $0 describe <topic_name>"
      exit 1
    fi
    describe_topic $2
    ;;
  *)
    echo "Usage:"
    echo "  $0 create <topic_name> <partitions> <replication_factor>"
    echo "  $0 list"
    echo "  $0 describe <topic_name>"
    exit 1
    ;;
esac