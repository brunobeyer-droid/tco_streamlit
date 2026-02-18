#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/refresh_all_snapshots.sh [YEAR]
# Example:
#   ./scripts/refresh_all_snapshots.sh 2026

YEAR="${1:-$(date +%Y)}"

echo "Refreshing all snapshots for YEAR=${YEAR} (and prior year window)..."

./scripts/refresh_velocity_snapshot.sh "${YEAR}" 1 0
./scripts/refresh_projected_demand_snapshot.sh "${YEAR}" 1 0

echo "Done."

