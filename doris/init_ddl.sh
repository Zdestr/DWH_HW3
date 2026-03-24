#!/bin/bash
set -e

echo "Waiting for Doris FE to be ready..."
until mysql -h doris-fe -P 9030 -u root --connect-timeout=5 \
    -e "SELECT 1" > /dev/null 2>&1; do
    echo "Doris FE not ready, retrying in 5s..."
    sleep 5
done

echo "Doris FE is ready. Running DDL..."
mysql -h doris-fe -P 9030 -u root < /dwh_detailed.sql
echo "DWH detailed DDL executed!"

mysql -h doris-fe -P 9030 -u root < /init_presentation.sql
echo "Presentation DDL executed!"

echo "All DDL executed successfully!"
