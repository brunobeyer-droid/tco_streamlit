#!/usr/bin/env python3
"""
Import ADO features from a CSV and upsert into ADO_FEATURES.

Reads MSSQL_* connection details from environment variables used by db facade.

Usage:
  python scripts/import_ado_features.py --file ado_features.csv [--chunk-size 1000]

Environment:
  MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD, [MSSQL_DRIVER]
"""
from __future__ import annotations

import argparse
import sys
from typing import List

import pandas as pd

from db import ensure_ado_minimal_tables, upsert_ado_features


REQUIRED_COLS: List[str] = [
    "FEATURE_ID",
    "TITLE",
    "STATE",
    "TEAM_RAW",
    "APP_NAME_RAW",
    "INVESTMENT_DIMENSION",
    "ITERATION_PATH",
    "CREATED_AT",
    "CHANGED_AT",
    "ADO_YEAR",
]
OPTIONAL_COLS: List[str] = [
    "PROGRAM_RAW",
    "AREA_LEVEL3_RAW",
    "AREA_LEVEL4_RAW",
    "AREA_PATH_RAW",
]


from typing import List, Optional


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Import ADO features from CSV and upsert into MSSQL")
    p.add_argument("--file", required=True, help="Input CSV (exported from export_ado_features.py)")
    p.add_argument("--chunk-size", type=int, default=1000, help="Upsert chunk size")
    p.add_argument("--dry-run", action="store_true", help="Parse and validate only; do not write")
    args = p.parse_args(argv)

    df = pd.read_csv(args.file)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        print(f"ERROR: missing expected columns: {missing}")
        return 2
    for col in OPTIONAL_COLS:
        if col not in df.columns:
            df[col] = None

    total = len(df)
    print(f"Loaded {total} rows from {args.file}")
    if args.dry_run:
        return 0

    ensure_ado_minimal_tables()
    n = 0
    chunk = int(max(1, args.chunk_size))
    for start in range(0, total, chunk):
        stop = min(start + chunk, total)
        part = df.iloc[start:stop]
        print(f"Upserting rows {start+1}-{stop} of {total}…", flush=True)
        n += upsert_ado_features(part)
    print(f"Done. Upserted {n} rows into ADO_FEATURES.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
