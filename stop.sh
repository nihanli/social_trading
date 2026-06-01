#!/usr/bin/env bash
# stop.sh — Kill all social_trading app processes (honcho + all services + Streamlit).
# Run this when Ctrl+C leaves orphaned processes behind.
#
# Usage:  ./stop.sh
#   or:   make stop

set -euo pipefail

# Patterns that identify app processes.  Matched against the full command line.
PATTERNS=(
    "honcho start"
    "social_trading.services"
    "streamlit run src/social_trading"
)

killed=0

for pattern in "${PATTERNS[@]}"; do
    # pgrep -f matches against full argument list; -l prints pid + name
    pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [[ -n "$pids" ]]; then
        echo "Stopping: $pattern"
        for pid in $pids; do
            echo "  kill $pid"
            kill "$pid" 2>/dev/null || true
        done
        killed=$((killed + $(echo "$pids" | wc -w | tr -d ' ')))
    fi
done

if [[ $killed -eq 0 ]]; then
    echo "Nothing to stop — no app processes found."
    exit 0
fi

# Wait up to 5 s for graceful shutdown, then force-kill survivors.
echo "Waiting for processes to exit..."
sleep 3

for pattern in "${PATTERNS[@]}"; do
    pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [[ -n "$pids" ]]; then
        echo "Force-killing stubborn processes for: $pattern"
        for pid in $pids; do
            echo "  kill -9 $pid"
            kill -9 "$pid" 2>/dev/null || true
        done
    fi
done

sleep 1
echo "Done. Remaining app processes:"
remaining=$(pgrep -f "social_trading.services\|honcho start\|streamlit run src/social_trading" 2>/dev/null || true)
if [[ -z "$remaining" ]]; then
    echo "  (none)"
else
    ps -p "$remaining" -o pid,command 2>/dev/null || true
fi
