#!/bin/sh

DEBEZIUM_URL="http://debezium:8083"

echo "Waiting for Debezium to be ready..."
until curl -sf "${DEBEZIUM_URL}/connectors" > /dev/null; do
    echo "Debezium not ready, retrying in 5s..."
    sleep 5
done

echo "Debezium is ready. Registering connectors..."

FAILED=0

for connector in /connectors/*.json; do
    echo "Registering: ${connector}"
    RESPONSE=$(curl -sf -X POST \
        -H "Content-Type: application/json" \
        --data @"${connector}" \
        "${DEBEZIUM_URL}/connectors" 2>&1)
    STATUS=$?

    if [ $STATUS -ne 0 ]; then
        # Проверяем — может коннектор уже существует
        CONNECTOR_NAME=$(cat "${connector}" | grep '"name"' | head -1 | sed 's/.*"name"[^"]*"\([^"]*\)".*/\1/')
        EXISTS=$(curl -sf "${DEBEZIUM_URL}/connectors/${CONNECTOR_NAME}" 2>/dev/null)
        if [ -n "$EXISTS" ]; then
            echo "Connector ${CONNECTOR_NAME} already exists, skipping"
        else
            echo "ERROR registering ${connector}: ${RESPONSE}"
            FAILED=1
        fi
    else
        echo "OK: ${RESPONSE}"
    fi
done

if [ $FAILED -eq 1 ]; then
    echo "Some connectors failed to register!"
    exit 1
fi

echo "All connectors registered successfully!"
exit 0
