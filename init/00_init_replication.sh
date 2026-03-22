#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ${REPLICATION_USER} WITH REPLICATION ENCRYPTED PASSWORD '${REPLICATION_PASSWORD}';
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT pg_create_physical_replication_slot('replica_slot');
EOSQL

echo "host replication ${REPLICATION_USER} all md5" >> "$PGDATA/pg_hba.conf"

cat >> "$PGDATA/postgresql.conf" <<EOF

wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
wal_keep_size = 256
hot_standby = on
EOF

pg_ctl reload -D "$PGDATA"
