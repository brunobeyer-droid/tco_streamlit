from __future__ import annotations

from typing import Any, List, Sequence, Optional

import pandas as pd
import streamlit as st

from core.cache_utils import cache_data_portfolio

from core.ado_recon import load_explorer_fte_by_group
from db import fetch_df_active as fetch_df  # type: ignore


def _empty_capacity_demand_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "YEAR",
            "PI",
            "ITERATION_LEVEL3",
            "PROGRAMNAME",
            "TEAMNAME",
            "HEADCOUNT",
            "CAPACITY_FTE",
            "DEMAND_FTE",
            "UTILIZATION_PCT",
            "CAPACITY_SOD_FTE",
            "DEMAND_SOD_FTE",
            "UTILIZATION_SOD",
        ]
    )


def _is_timeout_error(exc: Exception) -> bool:
    msg = str(exc or "").strip().lower()
    return ("timeout" in msg) or ("timed out" in msg) or ("hyt00" in msg)


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_projected_demand_snapshot_team_pi_years(
    *,
    years: Sequence[int],
    programs: Sequence[str],
    teams: Sequence[str],
) -> pd.DataFrame:
    years_norm = sorted({int(y) for y in years if y is not None and int(y) > 0})
    if not years_norm:
        return pd.DataFrame()
    where = [f"TRY_CONVERT(INT, YEAR) IN ({', '.join(['%s'] * len(years_norm))})"]
    params: List[Any] = [int(y) for y in years_norm]
    if programs:
        p_norm = [str(p).strip().upper() for p in programs if str(p).strip()]
        if p_norm:
            where.append(f"UPPER(COALESCE(PROGRAMNAME, '')) IN ({', '.join(['%s'] * len(p_norm))})")
            params.extend(p_norm)
    if teams:
        t_norm = [str(t).strip().upper() for t in teams if str(t).strip()]
        if t_norm:
            where.append(f"UPPER(COALESCE(TEAMNAME, '')) IN ({', '.join(['%s'] * len(t_norm))})")
            params.extend(t_norm)
    sql = f"""
      SELECT
        TRY_CONVERT(INT, YEAR) AS ADO_YEAR,
        TRY_CONVERT(INT, PI) AS PI_NUM,
        COALESCE(NULLIF(LTRIM(RTRIM(PROGRAMNAME)), ''), '(Unassigned)') AS PROGRAMNAME,
        COALESCE(NULLIF(LTRIM(RTRIM(TEAMNAME)), ''), '(Unassigned)') AS TEAMNAME,
        CAST(SUM(COALESCE(TRY_CONVERT(FLOAT, DERIVED_FTE_SUM), 0.0)) AS FLOAT) AS DERIVED_FTE_SUM
      FROM TCO_PROJECTED_DEMAND_SNAPSHOT
      WHERE {" AND ".join(where)}
      GROUP BY
        TRY_CONVERT(INT, YEAR),
        TRY_CONVERT(INT, PI),
        COALESCE(NULLIF(LTRIM(RTRIM(PROGRAMNAME)), ''), '(Unassigned)'),
        COALESCE(NULLIF(LTRIM(RTRIM(TEAMNAME)), ''), '(Unassigned)')
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["ADO_YEAR"] = pd.to_numeric(out.get("ADO_YEAR"), errors="coerce").astype("Int64")
    out["PI_NUM"] = pd.to_numeric(out.get("PI_NUM"), errors="coerce").astype("Int64")
    out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    out["TEAMNAME"] = out.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    out["DERIVED_FTE_SUM"] = pd.to_numeric(out.get("DERIVED_FTE_SUM"), errors="coerce").fillna(0.0)
    out = out[out["ADO_YEAR"].notna() & out["PI_NUM"].notna()].copy()
    return out


@cache_data_portfolio(ttl=300, show_spinner=False)
def _has_allocated_capacity_view() -> bool:
    """Return True when VW_TEAM_ALLOCATED_HEADCOUNT_PI exists (view or table)."""
    try:
        probe = fetch_df(
            """
            SELECT TOP 1 1 AS HAS_OBJ
            FROM sys.objects
            WHERE name = 'VW_TEAM_ALLOCATED_HEADCOUNT_PI'
              AND type IN ('V', 'U')
            """,
            None,
        )
        return bool(isinstance(probe, pd.DataFrame) and not probe.empty)
    except Exception:
        return False


def _roadmap_capacity_fraction(default: float = 0.8) -> float:
    """Capacity-side only: clamp roadmap_capacity_fraction to [0.3, 1.0]."""
    raw: Any = None
    try:
        raw = st.secrets.get("model", {}).get("roadmap_capacity_fraction", None)  # type: ignore[attr-defined]
    except Exception:
        raw = None
    try:
        val = float(raw) if raw is not None and str(raw).strip() != "" else float(default)
    except Exception:
        val = float(default)
    if not (val == val) or val in (float("inf"), float("-inf")):
        val = float(default)
    return max(0.3, min(1.0, float(val)))


@cache_data_portfolio(ttl=180, show_spinner=False)
def _load_allocated_capacity_team_pi_years(
    *,
    years: Sequence[int],
    programs: Sequence[str],
    teams: Sequence[str],
) -> pd.DataFrame:
    if not _has_allocated_capacity_view():
        # Avoid repeated SQL errors in environments where the view is not deployed.
        return pd.DataFrame()

    years = sorted({int(y) for y in years if y is not None and int(y) > 0})
    if not years:
        return pd.DataFrame()
    where = [f"ADO_YEAR IN ({', '.join(['%s'] * len(years))})"]
    params: List[Any] = list(years)
    if programs:
        placeholders = ", ".join(["%s"] * len(programs))
        where.append(f"UPPER(PROGRAMNAME) IN ({placeholders})")
        params.extend([str(p).upper() for p in programs])
    if teams:
        placeholders = ", ".join(["%s"] * len(teams))
        where.append(f"UPPER(TEAMNAME) IN ({placeholders})")
        params.extend([str(t).upper() for t in teams])
    sql = f"""
      SELECT
        TEAMID,
        TEAMNAME,
        PROGRAMID,
        PROGRAMNAME,
        ADO_YEAR,
        ITERATION_NUM,
        DELIVERY_HEADCOUNT,
        CONTRACTOR_C_HEADCOUNT,
        CONTRACTOR_CS_HEADCOUNT,
        ALLOCATED_HEADCOUNT
      FROM VW_TEAM_ALLOCATED_HEADCOUNT_PI
      WHERE {" AND ".join(where)}
    """
    try:
        df = fetch_df(sql, tuple(params) if params else None)
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@cache_data_portfolio(ttl=180, show_spinner=False)
def _fallback_capacity_from_headcount(
    *,
    years: Sequence[int],
    programs: Sequence[str],
    teams: Sequence[str],
    demand_multi: pd.DataFrame,
) -> pd.DataFrame:
    """Fallback capacity scaffold when `VW_TEAM_ALLOCATED_HEADCOUNT_PI` is empty/missing.

    This implementation is extracted from `pages/Budget.py` (Capacity tab) to ensure
    Dashboard and Budget use the exact same capacity source and PI fill rules.

    Returned columns align with `VW_TEAM_ALLOCATED_HEADCOUNT_PI`:
      TEAMID, TEAMNAME, PROGRAMID, PROGRAMNAME, ADO_YEAR, ITERATION_NUM,
      DELIVERY_HEADCOUNT, CONTRACTOR_CS_HEADCOUNT, CONTRACTOR_C_HEADCOUNT, ALLOCATED_HEADCOUNT
    """

    years_norm = sorted({int(y) for y in (years or []) if y is not None and int(y) > 0})
    if not years_norm:
        return pd.DataFrame()

    dem = demand_multi.copy() if demand_multi is not None else pd.DataFrame()
    if dem is None:
        dem = pd.DataFrame()
    if not dem.empty:
        dem["_Y"] = pd.to_numeric(dem.get("ADO_YEAR"), errors="coerce").astype("Int64")
        dem["_P"] = dem.get("PROGRAMNAME", "").astype(str).str.strip()
        dem["_T"] = dem.get("TEAMNAME", "").astype(str).str.strip()

    programs_up = [str(p).strip().upper() for p in (programs or []) if str(p).strip()]
    teams_up = [str(t).strip().upper() for t in (teams or []) if str(t).strip()]

    out_frames: List[pd.DataFrame] = []
    for yr in years_norm:
        eff_programs = programs_up
        eff_teams = teams_up
        if not eff_programs and not eff_teams and not dem.empty:
            dem_y = dem.loc[dem["_Y"].fillna(-1).astype(int) == int(yr)]
            eff_programs = sorted({str(x).strip().upper() for x in dem_y["_P"].dropna().tolist() if str(x).strip()})
            eff_teams = sorted({str(x).strip().upper() for x in dem_y["_T"].dropna().tolist() if str(x).strip()})

        if not eff_programs and not eff_teams and dem.empty:
            continue

        where_staff = [
            "TRY_CONVERT(INT, h.YEAR) = %s",
            "ISNULL(TRY_CONVERT(INT, h.PI), 0) BETWEEN 0 AND 4",
            "UPPER(h.CLASS) IN ('TEAM','DELIVERY')",
            "t.TEAMID IS NOT NULL",
        ]
        params_staff: List[Any] = [int(yr)]

        where_cons = [
            "TRY_CONVERT(INT, h.YEAR) = %s",
            "ISNULL(TRY_CONVERT(INT, h.PI), 0) BETWEEN 0 AND 4",
            "UPPER(h.CLASS) IN ('CONTRACTOR_CS','CONTRACTOR_C')",
            "t.TEAMID IS NOT NULL",
        ]
        params_cons: List[Any] = [int(yr)]

        if eff_programs:
            placeholders = ", ".join(["%s"] * len(eff_programs))
            clause = f"UPPER(p.PROGRAMNAME) IN ({placeholders})"
            where_staff.append(clause)
            where_cons.append(clause)
            params_staff.extend(eff_programs)
            params_cons.extend(eff_programs)

        if eff_teams:
            placeholders = ", ".join(["%s"] * len(eff_teams))
            clause = f"UPPER(t.TEAMNAME) IN ({placeholders})"
            where_staff.append(clause)
            where_cons.append(clause)
            params_staff.extend(eff_teams)
            params_cons.extend(eff_teams)

        sql_staff = f"""
          SELECT
            t.TEAMID,
            t.TEAMNAME,
            t.PROGRAMID,
            p.PROGRAMNAME,
            TRY_CONVERT(INT, h.YEAR) AS ADO_YEAR,
            TRY_CONVERT(INT, h.PI) AS PI,
            SUM(COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0)) AS STAFF_HEADCOUNT
          FROM TEAM_HEADCOUNT_HISTORY h
          LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
          LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
          WHERE {" AND ".join(where_staff)}
          GROUP BY t.TEAMID, t.TEAMNAME, t.PROGRAMID, p.PROGRAMNAME, TRY_CONVERT(INT, h.YEAR), TRY_CONVERT(INT, h.PI)
        """

        sql_cons = f"""
          SELECT
            t.TEAMID,
            t.TEAMNAME,
            t.PROGRAMID,
            p.PROGRAMNAME,
            TRY_CONVERT(INT, h.YEAR) AS ADO_YEAR,
            TRY_CONVERT(INT, h.PI) AS PI,
            SUM(CASE WHEN UPPER(h.CLASS) = 'CONTRACTOR_CS' THEN COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0) ELSE 0 END) AS CONTRACTOR_CS_HEADCOUNT,
            SUM(CASE WHEN UPPER(h.CLASS) = 'CONTRACTOR_C' THEN COALESCE(TRY_CONVERT(FLOAT, h.HEADCOUNT), 0) ELSE 0 END) AS CONTRACTOR_C_HEADCOUNT
          FROM TEAM_CONTRACTOR_HEADCOUNT h
          LEFT JOIN TEAMS t ON t.TEAMID = h.TEAMID
          LEFT JOIN PROGRAMS p ON p.PROGRAMID = t.PROGRAMID
          WHERE {" AND ".join(where_cons)}
          GROUP BY t.TEAMID, t.TEAMNAME, t.PROGRAMID, p.PROGRAMNAME, TRY_CONVERT(INT, h.YEAR), TRY_CONVERT(INT, h.PI)
        """

        try:
            staff = fetch_df(sql_staff, tuple(params_staff) if params_staff else None)
        except Exception:
            staff = None
        try:
            cons = fetch_df(sql_cons, tuple(params_cons) if params_cons else None)
        except Exception:
            cons = None

        staff = staff if staff is not None else pd.DataFrame()
        cons = cons if cons is not None else pd.DataFrame()

        for df0 in (staff, cons):
            if df0 is None or df0.empty:
                continue
            df0["TEAMNAME"] = df0.get("TEAMNAME", "").astype(str).str.strip()
            df0["PROGRAMNAME"] = df0.get("PROGRAMNAME", "").astype(str).str.strip()
            df0["ADO_YEAR"] = pd.to_numeric(df0.get("ADO_YEAR"), errors="coerce").astype("Int64")
            df0["PI"] = pd.to_numeric(df0.get("PI"), errors="coerce").astype("Int64")

        if not staff.empty:
            staff = staff[staff["TEAMNAME"].ne("") & staff["ADO_YEAR"].notna()].copy()
            staff = staff.rename(columns={"STAFF_HEADCOUNT": "DELIVERY_HEADCOUNT"})
            staff["DELIVERY_HEADCOUNT"] = pd.to_numeric(staff.get("DELIVERY_HEADCOUNT"), errors="coerce").fillna(0.0)
        if not cons.empty:
            cons = cons[cons["TEAMNAME"].ne("") & cons["ADO_YEAR"].notna()].copy()
            cons["CONTRACTOR_CS_HEADCOUNT"] = pd.to_numeric(cons.get("CONTRACTOR_CS_HEADCOUNT"), errors="coerce").fillna(0.0)
            cons["CONTRACTOR_C_HEADCOUNT"] = pd.to_numeric(cons.get("CONTRACTOR_C_HEADCOUNT"), errors="coerce").fillna(0.0)

        keys = ["TEAMID", "TEAMNAME", "PROGRAMID", "PROGRAMNAME", "ADO_YEAR"]
        key_frames: List[pd.DataFrame] = []
        if not staff.empty:
            key_frames.append(staff[keys].drop_duplicates())
        if not cons.empty:
            key_frames.append(cons[keys].drop_duplicates())
        base_keys = pd.concat(key_frames, ignore_index=True).drop_duplicates() if key_frames else pd.DataFrame(columns=keys)
        if base_keys.empty:
            continue

        def _pivot(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame()
            w = df[keys + ["PI", value_col]].copy()
            w[value_col] = pd.to_numeric(w.get(value_col), errors="coerce").fillna(0.0)
            return w.pivot_table(index=keys, columns="PI", values=value_col, aggfunc="sum", fill_value=0.0)

        p_staff = _pivot(staff, "DELIVERY_HEADCOUNT")
        p_cs = _pivot(cons, "CONTRACTOR_CS_HEADCOUNT")
        p_c = _pivot(cons, "CONTRACTOR_C_HEADCOUNT")

        frames_year: List[pd.DataFrame] = []
        for pi in (1, 2, 3, 4):
            row = base_keys.reset_index(drop=True).copy()
            row["ITERATION_NUM"] = int(pi)

            def _series(pvt: pd.DataFrame) -> pd.Series:
                if pvt is None or pvt.empty:
                    return pd.Series([0.0] * len(row.index), index=row.index)
                base = row.merge(pvt.reset_index(), on=keys, how="left")
                col0 = base[0] if 0 in base.columns else 0.0
                colp = base[pi] if pi in base.columns else col0
                colp = pd.to_numeric(colp, errors="coerce")
                col0 = pd.to_numeric(col0, errors="coerce")
                vals = colp.fillna(col0.fillna(0.0)).astype(float).to_numpy()
                return pd.Series(vals, index=row.index, dtype=float)

            row["DELIVERY_HEADCOUNT"] = _series(p_staff).astype(float)
            row["CONTRACTOR_CS_HEADCOUNT"] = _series(p_cs).astype(float)
            row["CONTRACTOR_C_HEADCOUNT"] = _series(p_c).astype(float)
            row["ALLOCATED_HEADCOUNT"] = (
                pd.to_numeric(row["DELIVERY_HEADCOUNT"], errors="coerce").fillna(0.0)
                + pd.to_numeric(row["CONTRACTOR_CS_HEADCOUNT"], errors="coerce").fillna(0.0)
                + pd.to_numeric(row["CONTRACTOR_C_HEADCOUNT"], errors="coerce").fillna(0.0)
            )

            row["ADO_YEAR"] = pd.to_numeric(row.get("ADO_YEAR"), errors="coerce").astype("Int64")
            row["ITERATION_NUM"] = pd.to_numeric(row.get("ITERATION_NUM"), errors="coerce").astype("Int64")
            frames_year.append(
                row[
                    [
                        "TEAMID",
                        "TEAMNAME",
                        "PROGRAMID",
                        "PROGRAMNAME",
                        "ADO_YEAR",
                        "ITERATION_NUM",
                        "DELIVERY_HEADCOUNT",
                        "CONTRACTOR_CS_HEADCOUNT",
                        "CONTRACTOR_C_HEADCOUNT",
                        "ALLOCATED_HEADCOUNT",
                    ]
                ]
            )

        out_y = pd.concat(frames_year, ignore_index=True) if frames_year else pd.DataFrame()
        out_y["ALLOCATED_HEADCOUNT"] = pd.to_numeric(out_y.get("ALLOCATED_HEADCOUNT"), errors="coerce").fillna(0.0)
        out_y = out_y[out_y["ALLOCATED_HEADCOUNT"] > 0].copy()
        out_frames.append(out_y)

    if not out_frames:
        return pd.DataFrame()
    out_all = pd.concat(out_frames, ignore_index=True)
    out_all["TEAMNAME"] = out_all.get("TEAMNAME", "").astype(str).str.strip()
    out_all["PROGRAMNAME"] = out_all.get("PROGRAMNAME", "").astype(str).str.strip()
    out_all["ADO_YEAR"] = pd.to_numeric(out_all.get("ADO_YEAR"), errors="coerce").astype("Int64")
    out_all["ITERATION_NUM"] = pd.to_numeric(out_all.get("ITERATION_NUM"), errors="coerce").astype("Int64")
    out_all["ALLOCATED_HEADCOUNT"] = pd.to_numeric(out_all.get("ALLOCATED_HEADCOUNT"), errors="coerce").fillna(0.0)
    out_all = out_all[
        out_all["TEAMNAME"].ne("") & out_all["ADO_YEAR"].notna() & out_all["ITERATION_NUM"].notna()
    ].copy()
    return out_all


@cache_data_portfolio(ttl=180, show_spinner=False)
def fetch_capacity_demand_pi(year: int, programs: list[str], teams: list[str], data_version: Optional[int] = None) -> pd.DataFrame:
    _ = data_version
    """Return the same Demand vs Capacity dataset as Budget → Capacity tab (Data Readiness table).

    Notes:
    - Demand comes from Explorer v2 derived FTE (VW_TCO_FEATURE_DEMAND aggregation).
    - Capacity comes from VW_TEAM_ALLOCATED_HEADCOUNT_PI (Delivery + Contractors), scaled by roadmap_capacity_fraction.
    - Includes the same fallback behavior as Budget when the capacity view is empty/missing rows.
    """
    y = int(year)
    pi_nums = [1, 2, 3, 4]
    programs_norm = [str(p).strip() for p in (programs or []) if str(p).strip()]
    teams_norm = [str(t).strip() for t in (teams or []) if str(t).strip()]

    demand_multi = _load_projected_demand_snapshot_team_pi_years(
        years=[y],
        programs=programs_norm,
        teams=teams_norm,
    )
    # Fallback to Explorer aggregation only when snapshot demand is unavailable/empty.
    if demand_multi is None or demand_multi.empty:
        try:
            demand_multi = load_explorer_fte_by_group(
                years=[y],
                programs=programs_norm,
                teams=teams_norm,
                groups=(),
                pi_nums=pi_nums,
                data_version=data_version,
            )
        except Exception as exc:
            if _is_timeout_error(exc):
                try:
                    st.session_state["capacity_demand_timeout"] = True
                except Exception:
                    pass
            return _empty_capacity_demand_df()
        if demand_multi is None:
            demand_multi = pd.DataFrame()

    if demand_multi.empty:
        return _empty_capacity_demand_df()

    dem = demand_multi.copy()
    dem["ADO_YEAR"] = pd.to_numeric(dem.get("ADO_YEAR"), errors="coerce").astype("Int64")
    dem["PI_NUM"] = pd.to_numeric(dem.get("PI_NUM"), errors="coerce").astype("Int64")
    dem = dem[dem["PI_NUM"].notna() & dem["PI_NUM"].isin(pi_nums)].copy()
    dem["PROGRAMNAME"] = dem.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    dem["TEAMNAME"] = dem.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    dem["DEMAND_FTE"] = pd.to_numeric(dem.get("DERIVED_FTE_SUM"), errors="coerce").fillna(0.0)

    cap = _load_allocated_capacity_team_pi_years(years=[y], programs=programs_norm, teams=teams_norm)
    cap = cap if cap is not None else pd.DataFrame()

    try:
        dem_need = dem.loc[dem["DEMAND_FTE"] > 0, ["ADO_YEAR", "PI_NUM", "PROGRAMNAME", "TEAMNAME"]].drop_duplicates()
        dem_need["_PROG_KEY"] = dem_need["PROGRAMNAME"].astype(str).str.upper().str.strip().replace({"": "(UNASSIGNED)"})
        dem_need["_TEAM_KEY"] = dem_need["TEAMNAME"].astype(str).str.upper().str.strip().replace({"": "(UNASSIGNED)"})

        cap_need = cap.copy()
        if cap_need is not None and not cap_need.empty:
            cap_need["ADO_YEAR"] = pd.to_numeric(cap_need.get("ADO_YEAR"), errors="coerce").astype("Int64")
            cap_need["ITERATION_NUM"] = pd.to_numeric(cap_need.get("ITERATION_NUM"), errors="coerce").astype("Int64")
            cap_need["PROGRAMNAME"] = cap_need.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
            cap_need["TEAMNAME"] = cap_need.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
            cap_need = cap_need[cap_need["ITERATION_NUM"].notna() & cap_need["ITERATION_NUM"].isin(pi_nums)].copy()
            cap_need["_PROG_KEY"] = cap_need["PROGRAMNAME"].astype(str).str.upper().str.strip().replace({"": "(UNASSIGNED)"})
            cap_need["_TEAM_KEY"] = cap_need["TEAMNAME"].astype(str).str.upper().str.strip().replace({"": "(UNASSIGNED)"})

        if cap_need is None or cap_need.empty or "ADO_YEAR" not in cap_need.columns or "ITERATION_NUM" not in cap_need.columns:
            cap_keys = pd.DataFrame(columns=["ADO_YEAR", "PI_NUM", "_PROG_KEY", "_TEAM_KEY"])
        else:
            cap_keys = (
                cap_need.loc[
                    cap_need["ADO_YEAR"].notna() & cap_need["ITERATION_NUM"].notna(),
                    ["ADO_YEAR", "ITERATION_NUM", "_PROG_KEY", "_TEAM_KEY"],
                ]
                .drop_duplicates()
                .rename(columns={"ITERATION_NUM": "PI_NUM"})
            )
        missing = dem_need.merge(cap_keys, on=["ADO_YEAR", "PI_NUM", "_PROG_KEY", "_TEAM_KEY"], how="left", indicator=True)
        missing = missing.loc[missing["_merge"].ne("both")].copy()

        if cap.empty or not missing.empty:
            missing_teams = sorted(set(missing["TEAMNAME"].astype(str).tolist())) if not missing.empty else list(teams_norm or ())
            fb = _fallback_capacity_from_headcount(
                years=[y],
                programs=programs_norm,
                teams=missing_teams,
                demand_multi=demand_multi,
            )
            fb = fb if fb is not None else pd.DataFrame()
            if fb is not None and not fb.empty:
                fb2 = fb.copy()
                fb2["ADO_YEAR"] = pd.to_numeric(fb2.get("ADO_YEAR"), errors="coerce").astype("Int64")
                fb2["ITERATION_NUM"] = pd.to_numeric(fb2.get("ITERATION_NUM"), errors="coerce").astype("Int64")
                fb2["PROGRAMNAME"] = fb2.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
                fb2["TEAMNAME"] = fb2.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
                fb2 = fb2[fb2["ITERATION_NUM"].notna() & fb2["ITERATION_NUM"].isin(pi_nums)].copy()
                fb2["_PROG_KEY"] = fb2["PROGRAMNAME"].astype(str).str.upper().str.strip().replace({"": "(UNASSIGNED)"})
                fb2["_TEAM_KEY"] = fb2["TEAMNAME"].astype(str).str.upper().str.strip().replace({"": "(UNASSIGNED)"})

                if not missing.empty:
                    missing_keys = set(
                        (int(r["ADO_YEAR"]), int(r["PI_NUM"]), str(r["_PROG_KEY"]), str(r["_TEAM_KEY"]))
                        for _, r in missing.loc[missing["ADO_YEAR"].notna() & missing["PI_NUM"].notna()].iterrows()
                    )
                    fb2 = fb2.loc[
                        fb2.apply(
                            lambda r: (int(r["ADO_YEAR"]), int(r["ITERATION_NUM"]), str(r["_PROG_KEY"]), str(r["_TEAM_KEY"])) in missing_keys,
                            axis=1,
                        )
                    ].copy()
                if cap.empty:
                    cap = fb2.drop(columns=["_PROG_KEY", "_TEAM_KEY"], errors="ignore")
                else:
                    cap = pd.concat(
                        [cap, fb2.drop(columns=["_PROG_KEY", "_TEAM_KEY"], errors="ignore")],
                        ignore_index=True,
                    )
    except Exception:
        pass

    if cap is None or cap.empty:
        return pd.DataFrame(
            columns=[
                "YEAR",
                "PI",
                "ITERATION_LEVEL3",
                "PROGRAMNAME",
                "TEAMNAME",
                "HEADCOUNT",
                "CAPACITY_FTE",
                "DEMAND_FTE",
                "UTILIZATION_PCT",
                "CAPACITY_SOD_FTE",
                "DEMAND_SOD_FTE",
                "UTILIZATION_SOD",
            ]
        )

    cap_fraction = float(_roadmap_capacity_fraction())
    capw = cap.copy()
    capw["ADO_YEAR"] = pd.to_numeric(capw.get("ADO_YEAR"), errors="coerce").astype("Int64")
    capw["ITERATION_NUM"] = pd.to_numeric(capw.get("ITERATION_NUM"), errors="coerce").astype("Int64")
    capw = capw[capw["ITERATION_NUM"].notna() & capw["ITERATION_NUM"].isin(pi_nums)].copy()
    capw["PROGRAMNAME"] = capw.get("PROGRAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    capw["TEAMNAME"] = capw.get("TEAMNAME", "").astype(str).str.strip().replace({"": "(Unassigned)"})
    capw["ALLOCATED_HEADCOUNT"] = pd.to_numeric(capw.get("ALLOCATED_HEADCOUNT"), errors="coerce").fillna(0.0)
    capw["CAPACITY_FTE"] = capw["ALLOCATED_HEADCOUNT"] * float(cap_fraction)

    dem_team = dem.groupby(["ADO_YEAR", "PI_NUM", "PROGRAMNAME", "TEAMNAME"], dropna=False)[["DEMAND_FTE"]].sum().reset_index()
    cap_team = (
        capw.groupby(["ADO_YEAR", "ITERATION_NUM", "PROGRAMNAME", "TEAMNAME"], dropna=False)[["CAPACITY_FTE", "ALLOCATED_HEADCOUNT"]]
        .sum()
        .reset_index()
        .rename(columns={"ITERATION_NUM": "PI_NUM"})
    )
    merged = dem_team.merge(cap_team, on=["ADO_YEAR", "PI_NUM", "PROGRAMNAME", "TEAMNAME"], how="left")
    merged["CAPACITY_FTE"] = pd.to_numeric(merged.get("CAPACITY_FTE"), errors="coerce").fillna(0.0)
    merged["HEADCOUNT"] = pd.to_numeric(merged.get("ALLOCATED_HEADCOUNT"), errors="coerce").fillna(0.0)
    merged["UTILIZATION"] = merged.apply(
        lambda r: (float(r["DEMAND_FTE"]) / float(r["CAPACITY_FTE"])) if float(r["CAPACITY_FTE"]) > 0 else pd.NA,
        axis=1,
    )
    ycol = pd.to_numeric(merged.get("ADO_YEAR"), errors="coerce").astype("Int64")
    pcol = pd.to_numeric(merged.get("PI_NUM"), errors="coerce").astype("Int64")
    merged["ITERATION_LEVEL3"] = (ycol.astype(str) + " I" + pcol.astype(str)).where(ycol.notna() & pcol.notna(), "")
    merged["PI_ORDER"] = (ycol * 10 + pcol).astype("Int64")

    merged["YEAR"] = pd.to_numeric(merged.get("ADO_YEAR"), errors="coerce").astype("Int64")
    merged["PI"] = pd.to_numeric(merged.get("PI_NUM"), errors="coerce").astype("Int64")

    merged["CAPACITY_SOD_FTE"] = pd.to_numeric(merged.get("CAPACITY_FTE"), errors="coerce").fillna(0.0)
    merged["DEMAND_SOD_FTE"] = pd.to_numeric(merged.get("DEMAND_FTE"), errors="coerce").fillna(0.0)
    merged["UTILIZATION_SOD"] = merged.apply(
        lambda r: (float(r["DEMAND_SOD_FTE"]) / float(r["CAPACITY_SOD_FTE"])) if float(r["CAPACITY_SOD_FTE"]) > 0 else pd.NA,
        axis=1,
    )
    merged["UTILIZATION_PCT"] = merged.get("UTILIZATION")

    out = merged.copy()
    out = out.sort_values(["ADO_YEAR", "PI_ORDER", "PROGRAMNAME", "TEAMNAME"], na_position="last")
    return out[
        [
            "YEAR",
            "PI",
            "ITERATION_LEVEL3",
            "PROGRAMNAME",
            "TEAMNAME",
            "HEADCOUNT",
            "CAPACITY_FTE",
            "DEMAND_FTE",
            "UTILIZATION_PCT",
            "CAPACITY_SOD_FTE",
            "DEMAND_SOD_FTE",
            "UTILIZATION_SOD",
        ]
    ].copy()
