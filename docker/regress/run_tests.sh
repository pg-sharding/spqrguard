#!/bin/bash
set -e

# Initialize PostgreSQL
su postgres -c "pg_ctl init -D /var/lib/postgresql/data"

# Configure shared_preload_libraries to load spqrguard
echo "shared_preload_libraries = 'spqrguard'" >> /var/lib/postgresql/data/postgresql.conf

# Start PostgreSQL
su postgres -c "pg_ctl start -D /var/lib/postgresql/data -o '-c listen_addresses=localhost'"

# Wait for PostgreSQL to be ready
sleep 2

# Run regression tests
cd /spqrguard
if ! make USE_PGXS=1 installcheck PGUSER=postgres TAP_TESTS=; then
    if [ -f /spqrguard/regression.diffs ]; then
        echo "=== regression.diffs ==="
        cat /spqrguard/regression.diffs
    fi
    exit 1
fi