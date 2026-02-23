#!/bin/sh
# register-connector.sh
# Registers the Oracle CDC connector via the Kafka Connect REST API.
# Called automatically by the connector-init Docker service.

echo "Registering Oracle CDC connector..."

curl -s -X POST http://connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/config/connector-config.json

echo ""
echo "Registration complete. Check status with:"
echo "  curl http://connect:8083/connectors/oracle-cdc-connector/status"

# --- DLQ topic létrehozás ---
echo "Creating DLQ topics..."
for TABLE in CUSTOMERS ORDERS PRODUCTS PRODUCT_CATEGORIES PRODUCTS_X_CATEGORY; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic dlq.cdc.APP.${TABLE} \
    --partitions 1 \
    --replication-factor 1 \
    && echo "Topic dlq.cdc.APP.${TABLE} OK"
done

echo "All DLQ topics created."