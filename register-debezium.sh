#!/bin/bash
# register-debezium.sh - Auto-register Debezium connector on startup
# Run this script after docker compose up -d to wait for Debezium and register connector

set -e

DEBEZIUM_URL="http://debezium:8083"
CONNECTOR_NAME="postgres-taxi-connector"
MAX_RETRIES=30
RETRY_INTERVAL=5

echo "Waiting for Debezium Connect to be ready..."
for i in $(seq 1 $MAX_RETRIES); do
    if curl -s -f "${DEBEZIUM_URL}/" > /dev/null 2>&1; then
        echo "Debezium Connect is ready!"
        break
    fi
    echo "Attempt $i/$MAX_RETRIES: Debezium not ready yet..."
    sleep $RETRY_INTERVAL
done

# Check if connector already exists
EXISTING=$(curl -s "${DEBEZIUM_URL}/connectors/${CONNECTOR_NAME}" 2>/dev/null)
if [ "$EXISTING" != "" ] && [ "$EXISTING" != "null" ]; then
    echo "Connector '${CONNECTOR_NAME}' already exists. Deleting..."
    curl -s -X DELETE "${DEBEZIUM_URL}/connectors/${CONNECTOR_NAME}"
    sleep 2
fi

# Register the connector
echo "Registering connector '${CONNECTOR_NAME}'..."
RESPONSE=$(curl -s -X POST "${DEBEZIUM_URL}/connectors" \
    -H "Content-Type: application/json" \
    -d @/debezium.json)

if echo "$RESPONSE" | grep -q "error"; then
    echo "ERROR: Failed to register connector:"
    echo "$RESPONSE"
    exit 1
fi

echo "Connector registered successfully!"
echo "Status:"
curl -s "${DEBEZIUM_URL}/connectors/${CONNECTOR_NAME}/status" | head -100

echo ""
echo "Done! The CDC pipeline is now configured."
echo "Kafka should start receiving data from PostgreSQL shortly."
