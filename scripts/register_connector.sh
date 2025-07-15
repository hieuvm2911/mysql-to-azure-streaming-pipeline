#!/bin/bash
# Wait for Kafka Connect REST API to become available
until curl -sf http://localhost:8083/; do
  echo "Waiting for Kafka Connect to start..."
  sleep 5
done

# Register the Debezium MySQL connector
curl -i -X POST http://localhost:8083/connectors \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  -d @kafka/debezium_mysql_connector.json
