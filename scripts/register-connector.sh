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
