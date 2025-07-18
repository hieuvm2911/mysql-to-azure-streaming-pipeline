#!/bin/bash

# Register the Debezium MySQL connector
curl -i -X POST http://localhost:8083/connectors \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  -d @kafka/debezium_mysql_connector.json
