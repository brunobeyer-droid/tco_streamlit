#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Tuple


RECOMMENDED_FLAGS: Dict[str, str] = {
    "TCO_VELOCITY_SNAPSHOT_AUTO_REFRESH": "0",
    "TCO_VELOCITY_ALLOW_STALE_SNAPSHOT": "1",
    "TCO_PROJECTED_DEMAND_SNAPSHOT_ENABLED": "1",
    "TCO_PROJECTED_DEMAND_SNAPSHOT_AUTO_REFRESH": "0",
    "TCO_PROJECTED_DEMAND_SNAPSHOT_STRICT_SCOPE": "1",
    "TCO_PROJECTED_VELOCITY_FAILOPEN_TO_SWAG": "1",
    "TCO_EXPLORER_VELOCITY_FROM_SNAPSHOT": "1",
    "TCO_COST_QUERY_CIRCUIT_SECONDS": "30",
    "TCO_WELCOME_INLINE_FIDELITY": "0",
    "TCO_DQ_LIGHT_FILTER_OPTIONS": "1",
    "TCO_REFRESH_SNAPSHOTS_AFTER_ADO_SYNC": "1",
    "TCO_REFRESH_SNAPSHOTS_AFTER_OFFLINE_REBUILD": "1",
}


def _load_secrets_flags(secrets_path: Path) -> Dict[str, str]:
    if not secrets_path.exists():
        return {}
    try:
        import tomllib  # py3.11+

        content = secrets_path.read_text(encoding="utf-8")
        data = tomllib.loads(content)
    except Exception:
        return {}
    flags = data.get("flags", {}) if isinstance(data, dict) else {}
    out: Dict[str, str] = {}
    if isinstance(flags, dict):
        for k, v in flags.items():
            key = str(k).strip()
            if not key:
                continue
            out[key] = str(v).strip()
    return out


def _resolve_value(name: str, secrets_flags: Dict[str, str]) -> Tuple[str, str]:
    env_val = os.getenv(name, "")
    if str(env_val).strip() != "":
        return str(env_val).strip(), "env"
    sec_val = secrets_flags.get(name, "")
    if str(sec_val).strip() != "":
        return str(sec_val).strip(), "secrets"
    return "", "unset"


def main() -> int:
    parser = argparse.ArgumentParser(description="Check runtime flags against production baseline.")
    parser.add_argument(
        "--secrets",
        default=".streamlit/secrets.toml",
        help="Path to Streamlit secrets TOML used as fallback when env vars are unset.",
    )
    args = parser.parse_args()

    secrets_flags = _load_secrets_flags(Path(args.secrets))
    rows = []
    ok = True
    for name, expected in RECOMMENDED_FLAGS.items():
        current, source = _resolve_value(name, secrets_flags)
        status = "OK" if current == expected else "MISMATCH"
        if status != "OK":
            ok = False
        rows.append((name, expected, current or "(unset)", source, status))

    print("Runtime Flag Check")
    print("==================")
    print(f"{'FLAG':50} {'EXPECTED':10} {'CURRENT':14} {'SOURCE':8} STATUS")
    for name, expected, current, source, status in rows:
        print(f"{name:50} {expected:10} {current:14} {source:8} {status}")

    if ok:
        print("\nAll recommended flags match.")
        return 0

    print("\nOne or more flags differ from the recommended production baseline.")
    print("Update env vars (preferred in PROD) and rerun this check.")
    return 1


if __name__ == "__main__":
    sys.exit(main())

