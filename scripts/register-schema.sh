#!/bin/sh

SCHEMA_REGISTRY_URL="http://schema-registry:8081"
SCHEMA_FILE="/schemas/employee.avsc"
SUBJECT="employee_topic-value"

echo "Waiting for Schema Registry to be ready..."
until wget -q --spider "${SCHEMA_REGISTRY_URL}/subjects" 2>/dev/null; do
  echo "Schema Registry not ready yet, retrying in 5s..."
  sleep 5
done

echo "Schema Registry is ready. Registering schema..."

SCHEMA=$(cat "${SCHEMA_FILE}" | tr -d '\n' | sed 's/"/\\"/g')
PAYLOAD="{\"schema\": \"${SCHEMA}\"}"

RESPONSE=$(wget -q -O - \
  --header="Content-Type: application/vnd.schemaregistry.v1+json" \
  --post-data="${PAYLOAD}" \
  "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT}/versions")

echo "Response: ${RESPONSE}"
echo "Schema registration complete."

