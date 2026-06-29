#!/usr/bin/env bash
# scripts/sync_to_prod.sh — sync reference tables from trading_test → trading_prod
#
# Use this to promote social/sentiment/signal history from the test environment to
# production when you want prod to benefit from weeks of test-env signal learning.
#
# Tables synced (test → prod, replaces prod data):
#   social_raw, sentiment_scores, sentiment_aggregates, signals
#
# Tables NOT synced (always env-specific):
#   trades, positions, account_equity, config_runs
#
# Usage: ./scripts/sync_to_prod.sh

set -euo pipefail

SRC_CONTAINER=social_trading_test-postgres-1
DST_CONTAINER=social_trading_prod-postgres-1
SRC_DB=trading_test
DST_DB=trading_prod
DB_USER=trader

TABLES="social_raw sentiment_scores sentiment_aggregates signals"

# Verify containers are running
for c in "$SRC_CONTAINER" "$DST_CONTAINER"; do
    if ! docker inspect "$c" --format '{{.State.Running}}' 2>/dev/null | grep -q true; then
        echo "❌ Container $c is not running. Start infra first:"
        echo "   make test-infra && make prod-infra"
        exit 1
    fi
done

echo ""
echo "⚠️  This will OVERWRITE reference data in $DST_DB with data from $SRC_DB."
echo "   Affected tables: $TABLES"
echo "   NOT affected: trades, positions, account_equity"
echo ""
read -rp "Continue? [y/N] " confirm
[[ "${confirm,,}" == "y" ]] || { echo "Aborted."; exit 0; }

echo ""
for table in $TABLES; do
    echo "  syncing $table ..."
    docker exec "$SRC_CONTAINER" \
        pg_dump -U "$DB_USER" "$SRC_DB" \
        -t "$table" \
        --data-only \
        --disable-triggers \
        | docker exec -i "$DST_CONTAINER" \
        psql -U "$DB_USER" "$DST_DB" -q
    # Row count in dest after sync
    count=$(docker exec "$DST_CONTAINER" \
        psql -U "$DB_USER" "$DST_DB" -t -c "SELECT COUNT(*) FROM $table;")
    echo "    → $table: $count rows in prod"
done

echo ""
echo "✅ Sync complete."
