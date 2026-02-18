#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="python3"
if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
fi

"${PYTHON_BIN}" -m pytest -q \
  tests/test_authorization.py \
  tests/test_scope_master_pages.py \
  tests/test_ux_copy_contracts.py \
  tests/test_page_ux_smoke.py
