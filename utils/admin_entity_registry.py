from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from core.cache_utils import cache_data_portfolio
from db import fetch_df, _fq

# Admin Entity Registry
# Single source of truth for entities shown in Admin tabs.


def table_exists(table: str) -> bool:
    try:
        df = fetch_df(
            "SELECT COUNT(*) CNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = %s",
            (table,),
        )
        return int(df.iloc[0]["CNT"]) > 0 if isinstance(df, pd.DataFrame) and not df.empty else False
    except Exception:
        return False


def _row_count(table: str) -> Optional[int]:
    if not table_exists(table):
        return None
    try:
        df = fetch_df(f"SELECT COUNT(*) CNT FROM { _fq(table) }")
        if isinstance(df, pd.DataFrame) and not df.empty:
            return int(df.iloc[0]["CNT"])
    except Exception:
        return None
    return None


def get_entity_registry() -> List[Dict[str, Any]]:
    return [
        {
            "id": "Programs",
            "label": "Programs",
            "tables": ["PROGRAMS"],
            "pk": ["PROGRAMID"],
            "display_cols": ["PROGRAMNAME"],
            "supports": {"browse_manage": True, "bulk_edit": True, "delete": True, "cascade_delete": False},
        },
        {
            "id": "Teams",
            "label": "Teams",
            "tables": ["TEAMS"],
            "pk": ["TEAMID"],
            "display_cols": ["TEAMNAME"],
            "supports": {"browse_manage": True, "bulk_edit": True, "delete": True, "cascade_delete": False},
        },
        {
            "id": "Application Groups",
            "label": "Application Groups",
            "tables": ["APPLICATION_GROUPS"],
            "pk": ["GROUPID"],
            "display_cols": ["GROUPNAME"],
            "supports": {"browse_manage": True, "bulk_edit": True, "delete": True, "cascade_delete": False},
        },
        {
            "id": "Applications",
            "label": "Applications",
            "tables": ["APPLICATIONS"],
            "pk": ["APPLICATIONID"],
            "display_cols": ["APPLICATIONNAME"],
            "supports": {"browse_manage": True, "bulk_edit": True, "delete": True, "cascade_delete": False},
        },
        {
            "id": "Vendors",
            "label": "Vendors",
            "tables": ["VENDORS"],
            "pk": ["VENDORID"],
            "display_cols": ["VENDORNAME"],
            "supports": {"browse_manage": True, "bulk_edit": True, "delete": True, "cascade_delete": False},
        },
        {
            "id": "Invoices",
            "label": "Invoices",
            "tables": ["INVOICES"],
            "pk": ["INVOICEID"],
            "display_cols": ["INVOICEID"],
            "supports": {"browse_manage": True, "bulk_edit": True, "delete": True, "cascade_delete": False},
        },
        {
            "id": "Rates",
            "label": "Rates",
            "tables": ["TEAM_RATE_HISTORY", "PROGRAM_RATE_HISTORY"],
            "pk": ["RATE_TYPE", "OWNER_ID", "YEAR", "PI", "LOCATION"],
            "display_cols": ["RATE_TYPE", "OWNER_NAME", "YEAR", "LOCATION"],
            "supports": {"browse_manage": True, "bulk_edit": True, "delete": True, "cascade_delete": False},
        },
        {
            "id": "Contracts",
            "label": "Contracts",
            "tables": ["CONTRACTS"],
            "pk": ["CONTRACT_ID"],
            "display_cols": ["CONTRACT_ID"],
            "supports": {"browse_manage": True, "bulk_edit": False, "delete": True, "cascade_delete": False},
        },
        {
            "id": "User",
            "label": "Users / Access",
            "tables": ["APP_USERS"],
            "pk": ["EMAIL"],
            "display_cols": ["EMAIL"],
            "supports": {"browse_manage": True, "bulk_edit": False, "delete": False, "cascade_delete": False},
        },
    ]


def get_entity_by_id(entity_id: str) -> Optional[Dict[str, Any]]:
    for ent in get_entity_registry():
        if ent["id"] == entity_id:
            return ent
    return None


def list_entities_for(feature: str) -> List[Dict[str, Any]]:
    return [e for e in get_entity_registry() if e.get("supports", {}).get(feature)]


@cache_data_portfolio(ttl=120)
def get_registry_diagnostics() -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for ent in get_entity_registry():
        tables = ent.get("tables") or []
        missing = [t for t in tables if not table_exists(t)]
        exists = len(missing) == 0 if tables else False
        counts = {t: _row_count(t) for t in tables}
        rows.append(
            {
                "Entity": ent["label"],
                "Tables": ", ".join(tables),
                "Tables exist": exists,
                "Missing tables": ", ".join(missing),
                "Row counts": "; ".join(f"{k}={v}" for k, v in counts.items() if v is not None),
                "Browse": bool(ent.get("supports", {}).get("browse_manage")),
                "Bulk edit": bool(ent.get("supports", {}).get("bulk_edit")),
                "Delete": bool(ent.get("supports", {}).get("delete")),
            }
        )
    return pd.DataFrame(rows)
