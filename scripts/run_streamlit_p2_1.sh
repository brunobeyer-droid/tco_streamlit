#!/usr/bin/env bash
set -euo pipefail

# P2.1/P3 safe defaults for local/dev execution.
# If not already exported, try reading values from .streamlit/secrets.toml [flags].
_py_for_secrets=""
if [[ -x ".venv/bin/python" ]]; then
  _py_for_secrets=".venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  _py_for_secrets="python3"
fi

if [[ -n "${_py_for_secrets}" ]] && [[ -f ".streamlit/secrets.toml" ]]; then
  # Export only vars that are not already set in the shell.
  eval "$(
    "${_py_for_secrets}" - <<'PY'
import pathlib
import shlex
import tomllib

path = pathlib.Path(".streamlit/secrets.toml")
try:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
except Exception:
    data = {}

flags = data.get("flags", {})
if not isinstance(flags, dict):
    flags = {}

keys = [
    "TCO_VELOCITY_SNAPSHOT_AUTO_REFRESH",
    "TCO_COST_QUERY_CIRCUIT_SECONDS",
    "TCO_PROJECTED_DEMAND_SNAPSHOT_ENABLED",
    "TCO_PROJECTED_DEMAND_SNAPSHOT_AUTO_REFRESH",
    "TCO_PROJECTED_DEMAND_SNAPSHOT_STRICT_SCOPE",
    "TCO_PROJECTED_VELOCITY_FAILOPEN_TO_SWAG",
    "TCO_WELCOME_EXPECTED_ONLY_FASTLOAD",
]

for key in keys:
    if key in flags and flags[key] is not None:
        print(f'[[ -z "${{{key}:-}}" ]] && export {key}={shlex.quote(str(flags[key]))}')
PY
  )"
fi

export TCO_VELOCITY_SNAPSHOT_AUTO_REFRESH="${TCO_VELOCITY_SNAPSHOT_AUTO_REFRESH:-0}"
export TCO_COST_QUERY_CIRCUIT_SECONDS="${TCO_COST_QUERY_CIRCUIT_SECONDS:-30}"
export TCO_PROJECTED_DEMAND_SNAPSHOT_ENABLED="${TCO_PROJECTED_DEMAND_SNAPSHOT_ENABLED:-1}"
export TCO_PROJECTED_DEMAND_SNAPSHOT_AUTO_REFRESH="${TCO_PROJECTED_DEMAND_SNAPSHOT_AUTO_REFRESH:-0}"
export TCO_PROJECTED_DEMAND_SNAPSHOT_STRICT_SCOPE="${TCO_PROJECTED_DEMAND_SNAPSHOT_STRICT_SCOPE:-1}"
export TCO_PROJECTED_VELOCITY_FAILOPEN_TO_SWAG="${TCO_PROJECTED_VELOCITY_FAILOPEN_TO_SWAG:-1}"
export TCO_WELCOME_EXPECTED_ONLY_FASTLOAD="${TCO_WELCOME_EXPECTED_ONLY_FASTLOAD:-0}"
export TCO_WELCOME_LIGHT_MODE="${TCO_WELCOME_LIGHT_MODE:-1}"
export TCO_PROJECTED_COST_FAILOPEN_SNAPSHOT="${TCO_PROJECTED_COST_FAILOPEN_SNAPSHOT:-1}"
export TCO_BASELINE_FAST_FROM_SPLIT="${TCO_BASELINE_FAST_FROM_SPLIT:-1}"
export TCO_SKIP_RUNTIME_SCHEMA_ENSURE="${TCO_SKIP_RUNTIME_SCHEMA_ENSURE:-1}"
export MSSQL_CONNECT_TIMEOUT="${MSSQL_CONNECT_TIMEOUT:-5}"
# Keep query timeout low enough for fail-open paths to kick in quickly under local DB pressure.
if [[ -z "${MSSQL_QUERY_TIMEOUT:-}" ]]; then
  export MSSQL_QUERY_TIMEOUT=8
else
  if [[ "${MSSQL_QUERY_TIMEOUT}" =~ ^[0-9]+$ ]] && (( MSSQL_QUERY_TIMEOUT > 12 )); then
    export MSSQL_QUERY_TIMEOUT=8
  fi
fi
export MSSQL_CONN_RETRY_SLEEP_MS="${MSSQL_CONN_RETRY_SLEEP_MS:-350}"
export TCO_SCHEMA_ENFORCE_PI_NOT_NULL="${TCO_SCHEMA_ENFORCE_PI_NOT_NULL:-0}"

echo "TCO_VELOCITY_SNAPSHOT_AUTO_REFRESH=${TCO_VELOCITY_SNAPSHOT_AUTO_REFRESH}"
echo "TCO_COST_QUERY_CIRCUIT_SECONDS=${TCO_COST_QUERY_CIRCUIT_SECONDS}"
echo "TCO_PROJECTED_DEMAND_SNAPSHOT_ENABLED=${TCO_PROJECTED_DEMAND_SNAPSHOT_ENABLED}"
echo "TCO_PROJECTED_DEMAND_SNAPSHOT_AUTO_REFRESH=${TCO_PROJECTED_DEMAND_SNAPSHOT_AUTO_REFRESH}"
echo "TCO_PROJECTED_DEMAND_SNAPSHOT_STRICT_SCOPE=${TCO_PROJECTED_DEMAND_SNAPSHOT_STRICT_SCOPE}"
echo "TCO_PROJECTED_VELOCITY_FAILOPEN_TO_SWAG=${TCO_PROJECTED_VELOCITY_FAILOPEN_TO_SWAG}"
echo "TCO_WELCOME_EXPECTED_ONLY_FASTLOAD=${TCO_WELCOME_EXPECTED_ONLY_FASTLOAD}"
echo "TCO_WELCOME_LIGHT_MODE=${TCO_WELCOME_LIGHT_MODE}"
echo "TCO_PROJECTED_COST_FAILOPEN_SNAPSHOT=${TCO_PROJECTED_COST_FAILOPEN_SNAPSHOT}"
echo "TCO_BASELINE_FAST_FROM_SPLIT=${TCO_BASELINE_FAST_FROM_SPLIT}"
echo "TCO_SKIP_RUNTIME_SCHEMA_ENSURE=${TCO_SKIP_RUNTIME_SCHEMA_ENSURE}"
echo "MSSQL_CONNECT_TIMEOUT=${MSSQL_CONNECT_TIMEOUT}"
echo "MSSQL_QUERY_TIMEOUT=${MSSQL_QUERY_TIMEOUT}"
echo "MSSQL_CONN_RETRY_SLEEP_MS=${MSSQL_CONN_RETRY_SLEEP_MS}"
echo "TCO_SCHEMA_ENFORCE_PI_NOT_NULL=${TCO_SCHEMA_ENFORCE_PI_NOT_NULL}"

if [[ -x ".venv/bin/python" ]]; then
  exec .venv/bin/python -m streamlit run main.py
fi

if command -v python3 >/dev/null 2>&1; then
  exec python3 -m streamlit run main.py
fi

exec streamlit run main.py
