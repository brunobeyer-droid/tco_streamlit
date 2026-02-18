from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, Optional

import pandas as pd


FetchFn = Callable[[str, Optional[Iterable[Any]]], pd.DataFrame]
ExecFn = Callable[[str, Optional[Iterable[Any]], bool], Any]


@dataclass(frozen=True)
class DefaultInstancesResult:
    groups_created: int
    groups_updated: int
    groups_repaired_multi_default: int


def ensure_default_instances(exec_sql: Callable[..., Any], fetch_df: FetchFn) -> DefaultInstancesResult:
    """Ensure each Application Group has exactly one default Application (instance).

    Repo terminology:
    - APPLICATION_GROUPS = App Group
    - APPLICATIONS = Application instances (invoices/contracts reference APPLICATIONID)

    Rules:
    - If a group has 0 instances, create a default instance with APPLICATIONID = "<GROUPID>__DEFAULT".
    - If a group has instances but no default, mark one as IS_DEFAULT = 1.
    - If a group has multiple defaults, keep one and clear the rest.
    """
    # Best-effort: ensure the column exists even if DB bootstrap wasn't run yet.
    try:
        exec_sql("ALTER TABLE APPLICATIONS ADD COLUMN IF NOT EXISTS IS_DEFAULT BIT", None, False)
    except Exception:
        pass
    try:
        exec_sql("UPDATE APPLICATIONS SET IS_DEFAULT = 0 WHERE IS_DEFAULT IS NULL", None, False)
    except Exception:
        pass

    df_groups = fetch_df(
        """
        SELECT GROUPID, GROUPNAME
        FROM APPLICATION_GROUPS
        WHERE GROUPID IS NOT NULL AND LTRIM(RTRIM(GROUPID)) <> ''
        """,
        None,
    )
    if df_groups is None or df_groups.empty:
        return DefaultInstancesResult(groups_created=0, groups_updated=0, groups_repaired_multi_default=0)

    df_apps = fetch_df(
        """
        SELECT APPLICATIONID, APPLICATIONNAME, GROUPID, IS_DEFAULT
        FROM APPLICATIONS
        """,
        None,
    )
    if df_apps is None:
        df_apps = pd.DataFrame(columns=["APPLICATIONID", "APPLICATIONNAME", "GROUPID", "IS_DEFAULT"])

    df_apps = df_apps.copy()
    for c in ["APPLICATIONID", "APPLICATIONNAME", "GROUPID"]:
        if c in df_apps.columns:
            df_apps[c] = df_apps[c].astype(str).str.strip()
    if "IS_DEFAULT" in df_apps.columns:
        df_apps["IS_DEFAULT"] = pd.to_numeric(df_apps["IS_DEFAULT"], errors="coerce").fillna(0).astype(int)
    else:
        df_apps["IS_DEFAULT"] = 0

    created = 0
    updated = 0
    repaired_multi = 0
    now = datetime.utcnow()

    for _, g in df_groups.iterrows():
        group_id = str(g.get("GROUPID") or "").strip()
        group_name = str(g.get("GROUPNAME") or "").strip()
        if not group_id:
            continue

        apps = df_apps[df_apps["GROUPID"] == group_id].copy()
        if apps.empty:
            default_app_id = f"{group_id}__DEFAULT"
            default_name = (group_name or group_id)
            default_name = (f"{default_name} (Default)")[:255]
            # Insert default instance if missing.
            exec_sql(
                """
                MERGE INTO APPLICATIONS t
                USING (SELECT %s AS APPLICATIONID, %s AS APPLICATIONNAME, %s AS GROUPID, %s AS IS_DEFAULT, %s AS UPDATED_AT) s
                ON t.APPLICATIONID = s.APPLICATIONID
                WHEN MATCHED THEN UPDATE SET
                  APPLICATIONNAME = COALESCE(NULLIF(s.APPLICATIONNAME, ''), t.APPLICATIONNAME),
                  GROUPID = COALESCE(NULLIF(s.GROUPID, ''), t.GROUPID),
                  IS_DEFAULT = 1,
                  UPDATED_AT = COALESCE(s.UPDATED_AT, SYSDATETIME())
                WHEN NOT MATCHED THEN INSERT (APPLICATIONID, APPLICATIONNAME, GROUPID, IS_DEFAULT, CREATED_AT, UPDATED_AT)
                  VALUES (s.APPLICATIONID, s.APPLICATIONNAME, s.GROUPID, 1, SYSDATETIME(), COALESCE(s.UPDATED_AT, SYSDATETIME()));
                """,
                (default_app_id, default_name, group_id, 1, now),
                False,
            )
            created += 1
            continue

        defaults = apps[apps["IS_DEFAULT"] == 1]
        if len(defaults) > 1:
            keep_id = None
            preferred = f"{group_id}__DEFAULT"
            if (apps["APPLICATIONID"] == preferred).any():
                keep_id = preferred
            else:
                keep_id = sorted(defaults["APPLICATIONID"].astype(str).tolist())[0]
            exec_sql(
                "UPDATE APPLICATIONS SET IS_DEFAULT = CASE WHEN APPLICATIONID = %s THEN 1 ELSE 0 END WHERE GROUPID = %s",
                (keep_id, group_id),
                False,
            )
            repaired_multi += 1
            updated += 1
            continue

        if len(defaults) == 1:
            continue

        # No default: pick a deterministic candidate.
        if len(apps) == 1:
            pick_id = str(apps.iloc[0]["APPLICATIONID"])
        else:
            preferred = f"{group_id}__DEFAULT"
            if (apps["APPLICATIONID"] == preferred).any():
                pick_id = preferred
            elif group_name and (apps["APPLICATIONNAME"].astype(str).str.upper() == group_name.upper()).any():
                pick_id = str(apps.loc[apps["APPLICATIONNAME"].astype(str).str.upper() == group_name.upper(), "APPLICATIONID"].iloc[0])
            else:
                pick_id = sorted(apps["APPLICATIONID"].astype(str).tolist())[0]

        exec_sql(
            "UPDATE APPLICATIONS SET IS_DEFAULT = CASE WHEN APPLICATIONID = %s THEN 1 ELSE 0 END WHERE GROUPID = %s",
            (pick_id, group_id),
            False,
        )
        updated += 1

    return DefaultInstancesResult(groups_created=created, groups_updated=updated, groups_repaired_multi_default=repaired_multi)


__all__ = ["DefaultInstancesResult", "ensure_default_instances"]

