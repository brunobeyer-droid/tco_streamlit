#!/usr/bin/env python3
"""
Export ADO_FEATURES (and optional ADO mappings) to CSV.

Reads MSSQL_* connection details from environment variables used by db facade.

Usage:
  # Export features only
  python scripts/export_ado_features.py --out ado_features.csv

  # Export features and mappings
  python scripts/export_ado_features.py --out ado_features.csv --with-mappings

Environment:
  MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD, [MSSQL_DRIVER]
"""
from __future__ import annotations

import argparse
import sys
from typing import List

import pandas as pd

from db import fetch_df


FEATURE_COLS: List[str] = [
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
    "PROGRAM_RAW",
    "AREA_LEVEL3_RAW",
    "AREA_LEVEL4_RAW",
    "AREA_PATH_RAW",
]


def export_features(out_path: str) -> int:
    q = (
        "SELECT FEATURE_ID, TITLE, STATE, TEAM_RAW, APP_NAME_RAW, INVESTMENT_DIMENSION, "
        "ITERATION_PATH, CREATED_AT, CHANGED_AT, ADO_YEAR, "
        "PROGRAM_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW, AREA_PATH_RAW "
        "FROM ADO_FEATURES"
    )
    df = fetch_df(q) or pd.DataFrame(columns=FEATURE_COLS)
    df = df[FEATURE_COLS] if not df.empty else df
    df.to_csv(out_path, index=False)
    print(f"Exported {len(df)} rows to {out_path}")
    return len(df)


def export_mappings() -> None:
    # Program mapping
    try:
        df_p = fetch_df("SELECT ADO_PROGRAM, PROGRAMID FROM MAP_ADO_PROGRAM_TO_TCO_PROGRAM")
        if df_p is not None and not df_p.empty:
            df_p.to_csv("map_ado_program.csv", index=False)
            print(f"Exported program mapping: {len(df_p)} rows -> map_ado_program.csv")
        else:
            print("No rows in MAP_ADO_PROGRAM_TO_TCO_PROGRAM")
    except Exception as e:
        print(f"WARN: could not export MAP_ADO_PROGRAM_TO_TCO_PROGRAM: {e}")
    # Team mapping
    try:
        df_t = fetch_df("SELECT ADO_TEAM_KEY, ADO_TEAM, PROGRAM_RAW, AREA_LEVEL3_RAW, AREA_LEVEL4_RAW, TEAMID FROM MAP_ADO_TEAM_TO_TCO_TEAM")
        if df_t is not None and not df_t.empty:
            df_t.to_csv("map_ado_team.csv", index=False)
            print(f"Exported team mapping: {len(df_t)} rows -> map_ado_team.csv")
        else:
            print("No rows in MAP_ADO_TEAM_TO_TCO_TEAM")
    except Exception as e:
        print(f"WARN: could not export MAP_ADO_TEAM_TO_TCO_TEAM: {e}")
    # App mapping
    try:
        df_a = fetch_df("SELECT ADO_APP, APP_GROUP FROM MAP_ADO_APP_TO_TCO_GROUP")
        if df_a is not None and not df_a.empty:
            df_a.to_csv("map_ado_app.csv", index=False)
            print(f"Exported app mapping: {len(df_a)} rows -> map_ado_app.csv")
        else:
            print("No rows in MAP_ADO_APP_TO_TCO_GROUP")
    except Exception as e:
        print(f"WARN: could not export MAP_ADO_APP_TO_TCO_GROUP: {e}")


from typing import List, Optional


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Export ADO_FEATURES and optional mappings to CSV")
    p.add_argument("--out", default="ado_features.csv", help="Output CSV for ADO_FEATURES")
    p.add_argument("--with-mappings", action="store_true", help="Also export ADO mapping tables to CSV")
    args = p.parse_args(argv)

    n = export_features(args.out)
    if args.with_mappings:
        export_mappings()
    return 0 if n >= 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
