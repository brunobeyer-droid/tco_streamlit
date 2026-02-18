#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/refresh_velocity_snapshot.sh [YEAR] [INCLUDE_PRIOR_YEAR] [REFERENCE_YEAR_ONLY]
# Example:
#   ./scripts/refresh_velocity_snapshot.sh 2026 1 0

YEAR="${1:-$(date +%Y)}"
INCLUDE_PRIOR_YEAR="${2:-1}"
REFERENCE_YEAR_ONLY="${3:-0}"

if [[ -x ".venv/bin/python" ]]; then
  PY=".venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY="python3"
else
  PY="python"
fi

echo "Refreshing velocity snapshot..."
echo "  YEAR=${YEAR}"
echo "  INCLUDE_PRIOR_YEAR=${INCLUDE_PRIOR_YEAR}"
echo "  REFERENCE_YEAR_ONLY=${REFERENCE_YEAR_ONLY}"

"${PY}" - <<'PY' "${YEAR}" "${INCLUDE_PRIOR_YEAR}" "${REFERENCE_YEAR_ONLY}"
import sys

from db import refresh_tco_team_velocity_snapshot

year = int(sys.argv[1])
include_prior_year = str(sys.argv[2]).strip().lower() in {"1", "true", "yes", "on"}
reference_year_only = str(sys.argv[3]).strip().lower() in {"1", "true", "yes", "on"}

ok = refresh_tco_team_velocity_snapshot(
    year=year,
    include_prior_year=include_prior_year,
    reference_year_only=reference_year_only,
)
print(f"refresh_tco_team_velocity_snapshot={bool(ok)}")
PY

