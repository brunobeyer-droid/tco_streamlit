from __future__ import annotations

import logging
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

import pandas as pd

from core.timegrain import allocate_monthly_to_pi

logger = logging.getLogger(__name__)

FetchFn = Callable[[str, Optional[Iterable[Any]]], pd.DataFrame]
Filters = Mapping[str, Any]

_CALENDAR_COLUMNS_CACHE: Optional[set[str]] = None
_PI_CALENDAR_ROOT_CACHE: dict[int, Optional[str]] = {}
_PI_CALENDAR_ROOT_LOGGED: dict[int, Optional[str]] = {}


def _resolve_fetch(db: Any) -> FetchFn:
    if callable(db):
        return db  # type: ignore[return-value]
    if hasattr(db, "fetch_df") and callable(getattr(db, "fetch_df")):
        return getattr(db, "fetch_df")
    raise TypeError("`db` must be a callable (sql, params)->DataFrame or expose a `fetch_df(sql, params)` method.")


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [v for v in value if v is not None and str(v).strip() != ""]
    return [value]


def _add_in(where: list[str], params: list[Any], col_expr: str, values: Sequence[Any], *, upper: bool = False) -> None:
    vals = [v for v in values if v is not None and str(v).strip() != ""]
    if not vals:
        return
    if upper:
        vals = [str(v).strip().upper() for v in vals]
        col_expr = f"UPPER({col_expr})"
    placeholders = ", ".join(["%s"] * len(vals))
    where.append(f"{col_expr} IN ({placeholders})")
    params.extend(vals)


def _month_key(year_val: int, month_val: int) -> str:
    return f"{int(year_val):04d}-{int(month_val):02d}"


def _to_int_series_compat(x: Any, default: int = 0):
    """Convert to an integer dtype safely across pandas versions."""
    s = pd.to_numeric(x, errors="coerce")
    if not isinstance(s, pd.Series):
        try:
            return int(default) if pd.isna(s) else int(s)
        except Exception:
            return int(default)
    try:
        int64_nullable = pd.Int64Dtype()  # type: ignore[attr-defined]
        return s.astype(int64_nullable)
    except Exception:
        return s.fillna(default).astype("int64")


def _iteration_calendar_columns(fetch: FetchFn) -> set[str]:
    global _CALENDAR_COLUMNS_CACHE
    if _CALENDAR_COLUMNS_CACHE is not None:
        return _CALENDAR_COLUMNS_CACHE
    try:
        df = fetch(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'ADO_ITERATION_CALENDAR'
            """,
            None,
        )
    except Exception:
        _CALENDAR_COLUMNS_CACHE = set()
        return _CALENDAR_COLUMNS_CACHE
    if df is None or df.empty or "COLUMN_NAME" not in df.columns:
        _CALENDAR_COLUMNS_CACHE = set()
        return _CALENDAR_COLUMNS_CACHE
    cols = {str(c).strip().upper() for c in df["COLUMN_NAME"].dropna().astype(str).tolist() if str(c).strip()}
    _CALENDAR_COLUMNS_CACHE = cols
    return cols


def _dominant_iteration_root(fetch: FetchFn, year: int) -> Optional[str]:
    try:
        y = int(year)
    except Exception:
        return None
    try:
        df = fetch(
            r"""
            SELECT TOP 1
              ROOT,
              COUNT(*) AS N
            FROM (
              SELECT
                LEFT(
                  TRY_CONVERT(NVARCHAR(4000), ITERATION_PATH),
                  NULLIF(CHARINDEX('\', TRY_CONVERT(NVARCHAR(4000), ITERATION_PATH) + '\'), 0) - 1
                ) AS ROOT
              FROM dbo.ADO_FEATURES
              WHERE TRY_CONVERT(INT, ADO_YEAR) = %s
                AND ITERATION_PATH IS NOT NULL
                AND LTRIM(RTRIM(TRY_CONVERT(NVARCHAR(4000), ITERATION_PATH))) <> ''
            ) x
            WHERE ROOT IS NOT NULL AND LTRIM(RTRIM(ROOT)) <> ''
            GROUP BY ROOT
            ORDER BY N DESC, ROOT ASC
            """,
            (y,),
        )
    except Exception:
        return None
    if df is None or df.empty:
        return None
    try:
        root = str(df.iloc[0]["ROOT"]).strip()
        return root if root else None
    except Exception:
        return None


def _resolve_pi_calendar_root(db: Any, year: int, filters: Optional[dict]) -> Optional[str]:
    fetch = _resolve_fetch(db)
    y = int(year)

    override = None
    f = filters or {}
    for k in ("pi_calendar_root", "pi_calendar_root_override", "calendar_root"):
        v = f.get(k)
        if v is not None and str(v).strip():
            override = str(v).strip()
            break

    st = None
    try:
        import streamlit as st  # type: ignore
    except Exception:
        st = None

    if override is None and st is not None:
        v = st.session_state.get("pi_calendar_root_override")
        if v is not None and str(v).strip():
            override = str(v).strip()

    if override is not None:
        if str(override).strip().upper().startswith("AUTO"):
            override = None
        else:
            _log_pi_calendar_root_once(y, override, st=st)
            return override

    if st is not None:
        cache = st.session_state.setdefault("_pi_calendar_root_auto", {})
        if isinstance(cache, dict) and y in cache:
            root = cache.get(y)
            _log_pi_calendar_root_once(y, root, st=st)
            return str(root).strip() if root is not None and str(root).strip() else None

    if y in _PI_CALENDAR_ROOT_CACHE:
        root = _PI_CALENDAR_ROOT_CACHE.get(y)
        _log_pi_calendar_root_once(y, root, st=st)
        return root

    root = _dominant_iteration_root(fetch, y)
    _PI_CALENDAR_ROOT_CACHE[y] = root
    if st is not None:
        try:
            st.session_state.setdefault("_pi_calendar_root_auto", {})[y] = root
        except Exception:
            pass
    _log_pi_calendar_root_once(y, root, st=st)
    return root


def _log_pi_calendar_root_once(year: int, root: Optional[str], *, st=None) -> None:
    y = int(year)
    root0 = str(root).strip() if root is not None and str(root).strip() else None
    if st is not None:
        key = f"_pi_calendar_root_logged_{y}"
        prev = st.session_state.get(key, "__MISSING__")
        if prev != root0:
            print(f"PI calendar root for {y}: {root0 or 'AUTO/None'}")
            st.session_state[key] = root0
        return
    if y not in _PI_CALENDAR_ROOT_LOGGED or _PI_CALENDAR_ROOT_LOGGED.get(y) != root0:
        print(f"PI calendar root for {y}: {root0 or 'AUTO/None'}")
        _PI_CALENDAR_ROOT_LOGGED[y] = root0


def load_pi_calendar(db: Any, years: list[int], calendar_root: Optional[str] = None) -> pd.DataFrame:
    """Load PI calendar rows from `dbo.ADO_ITERATION_CALENDAR` (ITERATION_GRAIN='PI')."""
    fetch = _resolve_fetch(db)
    years0 = sorted({int(y) for y in (years or []) if str(y).strip()})
    if not years0:
        return pd.DataFrame(columns=["YEAR", "PI_NAME", "START_DATE", "END_DATE"])
    ph = ", ".join(["%s"] * len(years0))
    root = str(calendar_root).strip() if calendar_root is not None and str(calendar_root).strip() else None
    root_pred = ""
    root_params: list[Any] = []
    if root:
        cols = _iteration_calendar_columns(fetch)
        if "ITERATION_PATH_ROOT" in cols:
            root_pred = " AND UPPER(LTRIM(RTRIM(ITERATION_PATH_ROOT))) = UPPER(%s) "
            root_params.append(root)
        elif "ITERATION_PATH" in cols:
            root_pred = " AND UPPER(TRY_CONVERT(NVARCHAR(4000), ITERATION_PATH)) LIKE UPPER(%s) "
            root_params.append(root + "\\%")
        elif "ITERATION_LEVEL1" in cols:
            root_pred = " AND UPPER(LTRIM(RTRIM(ITERATION_LEVEL1))) = UPPER(%s) "
            root_params.append(root)
        elif "ITERATION_LEVEL0" in cols:
            root_pred = " AND UPPER(LTRIM(RTRIM(ITERATION_LEVEL0))) = UPPER(%s) "
            root_params.append(root)
    sql = f"""
      SELECT
        TRY_CONVERT(INT, YEAR) AS YEAR,
        COALESCE(NULLIF(LTRIM(RTRIM(PI_NAME)), ''), NULLIF(LTRIM(RTRIM(ITERATION_NAME)), '')) AS PI_NAME,
        START_DATE,
        END_DATE
      FROM dbo.ADO_ITERATION_CALENDAR
      WHERE UPPER(LTRIM(RTRIM(COALESCE(ITERATION_GRAIN, '')))) = 'PI'
        AND TRY_CONVERT(INT, YEAR) IN ({ph})
        AND START_DATE IS NOT NULL AND END_DATE IS NOT NULL
        {root_pred}
    """
    df = fetch(sql, list(years0) + root_params)
    if df is None or df.empty:
        return pd.DataFrame(columns=["YEAR", "PI_NAME", "START_DATE", "END_DATE"])
    w = df.copy()
    w["YEAR"] = _to_int_series_compat(w.get("YEAR"))
    w["PI_NAME"] = (
        w.get("PI_NAME", "")
        .fillna("")
        .astype(str)
        .str.upper()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )
    w["START_DATE"] = pd.to_datetime(w.get("START_DATE"), errors="coerce")
    w["END_DATE"] = pd.to_datetime(w.get("END_DATE"), errors="coerce")
    w = w[w["YEAR"].notna() & (w["PI_NAME"] != "") & w["START_DATE"].notna() & w["END_DATE"].notna()].copy()

    # Defensive filter: keep only labels like "2025 I1" / "2025 I4".
    pi_ok = w["PI_NAME"].str.match(r"^\d{4}\sI\d+$", na=False)
    w = w[pi_ok].copy()

    # Print once per year if calendar looks polluted (expected ~4 PIs).
    for y in sorted({int(x) for x in w["YEAR"].dropna().astype(int).tolist()}):
        try:
            n = int(w.loc[w["YEAR"].astype(int).eq(int(y)), "PI_NAME"].nunique())
        except Exception:
            n = 0
        if n > 10:
            key = f"_pi_calendar_rowcount_warned_{int(y)}"
            try:
                import streamlit as st  # type: ignore

                if not st.session_state.get(key, False):
                    print(f"WARNING: PI calendar for {int(y)} returned {n} rows; expected ~4. Check ITERATION_GRAIN / PI_NAME filters.")
                    st.session_state[key] = True
            except Exception:
                if not _PI_CALENDAR_ROOT_LOGGED.get(int(y) * 1000 + 1):  # best-effort single-run flag
                    print(f"WARNING: PI calendar for {int(y)} returned {n} rows; expected ~4. Check ITERATION_GRAIN / PI_NAME filters.")
                    _PI_CALENDAR_ROOT_LOGGED[int(y) * 1000 + 1] = "WARNED"

    return w[["YEAR", "PI_NAME", "START_DATE", "END_DATE"]].drop_duplicates()


def get_program_nwf_actuals_monthly(
    db: Any,
    years: list[int],
    filters: Optional[dict] = None,
) -> pd.DataFrame:
    """Return Apptio NWF actuals aggregated at PROGRAM×YEAR×MONTH (monthly)."""
    fetch = _resolve_fetch(db)
    years0 = sorted({int(y) for y in (years or []) if str(y).strip()})
    if not years0:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH_KEY", "AMOUNT"])

    where: list[str] = ["1=1"]
    params: list[Any] = []
    _add_in(where, params, "a.FISCAL_YEAR", years0)

    programs = [str(p).strip() for p in _as_list((filters or {}).get("program")) if str(p).strip()]
    _add_in(
        where,
        params,
        "COALESCE(NULLIF(LTRIM(RTRIM(p.PROGRAM_DISPLAY_NAME)), ''), p.PROGRAMNAME)",
        programs,
        upper=True,
    )

    sql = f"""
      SELECT
        p.PROGRAMID,
        p.PROGRAMNAME,
        TRY_CONVERT(INT, a.FISCAL_YEAR) AS YEAR,
        CONCAT(
          TRY_CONVERT(INT, a.FISCAL_YEAR),
          '-',
          RIGHT('0' + CONVERT(VARCHAR(2), TRY_CONVERT(INT, a.MONTH)), 2)
        ) AS MONTH_KEY,
        CAST(SUM(COALESCE(TRY_CONVERT(FLOAT, a.AMOUNT), 0.0)) AS DECIMAL(18,2)) AS AMOUNT
      FROM APPTIO_ACTUALS a
      JOIN PROGRAM_APPTIO_WORKIDS w ON w.WORK_ID = a.WORK_ID
      JOIN PROGRAMS p ON p.PROGRAMID = w.PROGRAMID
      WHERE {" AND ".join(where)}
      GROUP BY
        p.PROGRAMID,
        p.PROGRAMNAME,
        TRY_CONVERT(INT, a.FISCAL_YEAR),
        TRY_CONVERT(INT, a.MONTH)
      ORDER BY YEAR, MONTH_KEY, PROGRAMNAME
    """
    out = fetch(sql, params)
    if out is None or out.empty:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH_KEY", "AMOUNT"])
    w = out.copy()
    w["PROGRAMID"] = w.get("PROGRAMID", "").fillna("").astype(str).str.strip()
    w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    w["YEAR"] = _to_int_series_compat(w.get("YEAR"))
    w["MONTH_KEY"] = w.get("MONTH_KEY", "").fillna("").astype(str).str.strip()
    w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
    w = w[w["YEAR"].notna() & (w["MONTH_KEY"] != "") & (w["PROGRAMNAME"] != "")].copy()
    return w[["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH_KEY", "AMOUNT"]]


def _get_program_nwf_baseline_monthly(
    db: Any,
    years: list[int],
    filters: Optional[dict] = None,
) -> pd.DataFrame:
    """Best-effort baseline/monthly NWF fallback (PROGRAM_ADDITIONAL_COSTS)."""
    fetch = _resolve_fetch(db)
    years0 = sorted({int(y) for y in (years or []) if str(y).strip()})
    if not years0:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH", "MONTH_KEY", "AMOUNT"])

    where: list[str] = ["1=1"]
    params: list[Any] = []
    _add_in(where, params, "pac.YEAR", years0)
    where.append("TRY_CONVERT(INT, pac.MONTH) BETWEEN 1 AND 12")

    programs = [str(p).strip() for p in _as_list((filters or {}).get("program")) if str(p).strip()]
    _add_in(where, params, "p.PROGRAMNAME", programs, upper=True)

    sql = f"""
      SELECT
        pac.PROGRAMID,
        p.PROGRAMNAME,
        TRY_CONVERT(INT, pac.YEAR) AS YEAR,
        TRY_CONVERT(INT, pac.MONTH) AS MONTH,
        CONCAT(
          TRY_CONVERT(INT, pac.YEAR),
          '-',
          RIGHT('0' + CONVERT(VARCHAR(2), TRY_CONVERT(INT, pac.MONTH)), 2)
        ) AS MONTH_KEY,
        CAST(SUM(COALESCE(TRY_CONVERT(FLOAT, pac.AMOUNT), 0.0)) AS DECIMAL(18,2)) AS AMOUNT
      FROM PROGRAM_ADDITIONAL_COSTS pac
      LEFT JOIN PROGRAMS p ON p.PROGRAMID = pac.PROGRAMID
      WHERE {" AND ".join(where)}
      GROUP BY
        pac.PROGRAMID,
        p.PROGRAMNAME,
        TRY_CONVERT(INT, pac.YEAR),
        TRY_CONVERT(INT, pac.MONTH)
    """
    out = fetch(sql, params)
    if out is None or out.empty:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH", "MONTH_KEY", "AMOUNT"])
    w = out.copy()
    w["PROGRAMID"] = w.get("PROGRAMID", "").fillna("").astype(str).str.strip()
    w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    w["YEAR"] = _to_int_series_compat(w.get("YEAR"))
    w["MONTH"] = _to_int_series_compat(w.get("MONTH"))
    w["MONTH_KEY"] = w.get("MONTH_KEY", "").fillna("").astype(str).str.strip()
    w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
    w = w[w["YEAR"].notna() & w["MONTH"].notna() & (w["PROGRAMNAME"] != "")].copy()
    return w[["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH", "MONTH_KEY", "AMOUNT"]]


def get_program_nwf_expected_monthly(
    db: Any,
    years: list[int],
    filters: Optional[dict] = None,
) -> pd.DataFrame:
    """Expected/Projected NWF (monthly) = copy of Actual NWF, with safe fallbacks for missing months."""
    actual = get_program_nwf_actuals_monthly(db, years=years, filters=filters)
    baseline = _get_program_nwf_baseline_monthly(db, years=years, filters=filters)

    if (actual is None or actual.empty) and (baseline is None or baseline.empty):
        return pd.DataFrame(
            columns=[
                "PROGRAMID",
                "PROGRAMNAME",
                "YEAR",
                "MONTH_KEY",
                "EXPECTED_AMOUNT",
                "SOURCE",
                "SCENARIO",
                "COST_CATEGORY",
                "NWF_FORECAST_METHOD",
            ]
        )

    key_parts: list[pd.DataFrame] = []
    if actual is not None and not actual.empty:
        key_parts.append(actual[["PROGRAMID", "PROGRAMNAME", "YEAR"]].copy())
    if baseline is not None and not baseline.empty:
        key_parts.append(baseline[["PROGRAMID", "PROGRAMNAME", "YEAR"]].copy())

    if not key_parts:
        return pd.DataFrame(
            columns=[
                "PROGRAMID",
                "PROGRAMNAME",
                "YEAR",
                "MONTH_KEY",
                "EXPECTED_AMOUNT",
                "SOURCE",
                "SCENARIO",
                "COST_CATEGORY",
                "NWF_FORECAST_METHOD",
            ]
        )

    base_keys = (
        pd.concat(key_parts, ignore_index=True, sort=False).drop_duplicates()
        if len(key_parts) > 1
        else key_parts[0].drop_duplicates().copy()
    )
    base_keys["YEAR"] = _to_int_series_compat(base_keys.get("YEAR"))
    base_keys["PROGRAMID"] = base_keys.get("PROGRAMID", "").fillna("").astype(str).str.strip()
    base_keys["PROGRAMNAME"] = base_keys.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    base_keys = base_keys[base_keys["YEAR"].notna() & (base_keys["PROGRAMNAME"] != "")].copy()
    if base_keys.empty:
        return pd.DataFrame(
            columns=[
                "PROGRAMID",
                "PROGRAMNAME",
                "YEAR",
                "MONTH_KEY",
                "EXPECTED_AMOUNT",
                "SOURCE",
                "SCENARIO",
                "COST_CATEGORY",
                "NWF_FORECAST_METHOD",
            ]
        )

    month_grid = pd.DataFrame({"MONTH": list(range(1, 13))})
    base_keys["__k"] = 1
    month_grid["__k"] = 1
    grid = base_keys.merge(month_grid, on="__k", how="inner").drop(columns=["__k"])
    grid["MONTH_KEY"] = grid.apply(lambda r: _month_key(int(r["YEAR"]), int(r["MONTH"])), axis=1)

    act = actual.copy() if actual is not None else pd.DataFrame()
    if not act.empty:
        act["MONTH_KEY"] = act.get("MONTH_KEY", "").fillna("").astype(str).str.strip()
        act["MONTH"] = _to_int_series_compat(act["MONTH_KEY"].str.slice(-2))
        act_amt = act[["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH", "AMOUNT"]].copy()
    else:
        act_amt = pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH", "AMOUNT"])

    bl = baseline.copy() if baseline is not None else pd.DataFrame()
    if not bl.empty:
        bl_amt = bl.rename(columns={"AMOUNT": "BASELINE_AMOUNT"})[["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH", "BASELINE_AMOUNT"]].copy()
    else:
        bl_amt = pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH", "BASELINE_AMOUNT"])

    work = (
        grid.merge(act_amt, on=["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH"], how="left")
        .merge(bl_amt, on=["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH"], how="left")
        .copy()
    )
    work["AMOUNT"] = pd.to_numeric(work.get("AMOUNT"), errors="coerce")
    work["BASELINE_AMOUNT"] = pd.to_numeric(work.get("BASELINE_AMOUNT"), errors="coerce")

    last_act = pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "MAX_ACTUAL_MONTH", "LAST_ACTUAL_AMOUNT"])
    if not act_amt.empty:
        act_sorted = act_amt.copy()
        act_sorted["MONTH"] = _to_int_series_compat(act_sorted.get("MONTH"))
        act_sorted = act_sorted[act_sorted["MONTH"].notna()].sort_values(["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH"])
        tail = act_sorted.groupby(["PROGRAMID", "PROGRAMNAME", "YEAR"], dropna=False).tail(1)
        last_act = tail.rename(columns={"MONTH": "MAX_ACTUAL_MONTH", "AMOUNT": "LAST_ACTUAL_AMOUNT"})[
            ["PROGRAMID", "PROGRAMNAME", "YEAR", "MAX_ACTUAL_MONTH", "LAST_ACTUAL_AMOUNT"]
        ].copy()
        last_act["MAX_ACTUAL_MONTH"] = _to_int_series_compat(last_act.get("MAX_ACTUAL_MONTH"))
        last_act["LAST_ACTUAL_AMOUNT"] = pd.to_numeric(last_act.get("LAST_ACTUAL_AMOUNT"), errors="coerce")

    work = work.merge(last_act, on=["PROGRAMID", "PROGRAMNAME", "YEAR"], how="left")
    work["LAST_ACTUAL_AMOUNT"] = pd.to_numeric(work.get("LAST_ACTUAL_AMOUNT"), errors="coerce")
    work["MAX_ACTUAL_MONTH"] = _to_int_series_compat(work.get("MAX_ACTUAL_MONTH"))
    work["MONTH"] = _to_int_series_compat(work.get("MONTH"))

    has_actual = work["AMOUNT"].notna()
    has_last = work["MAX_ACTUAL_MONTH"].notna() & work["LAST_ACTUAL_AMOUNT"].notna()
    is_future = has_last & work["MONTH"].notna() & (work["MONTH"] > work["MAX_ACTUAL_MONTH"])
    has_baseline = work["BASELINE_AMOUNT"].notna()

    work["EXPECTED_AMOUNT"] = 0.0
    work["NWF_FORECAST_METHOD"] = "ZERO"
    work.loc[has_baseline, "EXPECTED_AMOUNT"] = work.loc[has_baseline, "BASELINE_AMOUNT"]
    work.loc[has_baseline, "NWF_FORECAST_METHOD"] = "BASELINE"
    work["EXPECTED_AMOUNT"] = pd.to_numeric(work.get("EXPECTED_AMOUNT"), errors="coerce")
    work["LAST_ACTUAL_AMOUNT"] = pd.to_numeric(work.get("LAST_ACTUAL_AMOUNT"), errors="coerce")
    work.loc[is_future, "EXPECTED_AMOUNT"] = work.loc[is_future, "LAST_ACTUAL_AMOUNT"]
    work.loc[is_future, "NWF_FORECAST_METHOD"] = "CARRY_FORWARD"
    work.loc[has_actual, "EXPECTED_AMOUNT"] = work.loc[has_actual, "AMOUNT"]
    work.loc[has_actual, "NWF_FORECAST_METHOD"] = "ACTUAL"
    work["EXPECTED_AMOUNT"] = work["EXPECTED_AMOUNT"].fillna(0.0)

    out = work[["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH_KEY", "EXPECTED_AMOUNT", "NWF_FORECAST_METHOD"]].copy()
    out["EXPECTED_AMOUNT"] = pd.to_numeric(out.get("EXPECTED_AMOUNT"), errors="coerce").fillna(0.0).astype(float)
    out["SOURCE"] = "APPTIO"
    out["SCENARIO"] = "Projected"
    out["COST_CATEGORY"] = "NON_WORK_FORCE"
    return out[
        [
            "PROGRAMID",
            "PROGRAMNAME",
            "YEAR",
            "MONTH_KEY",
            "EXPECTED_AMOUNT",
            "SOURCE",
            "SCENARIO",
            "COST_CATEGORY",
            "NWF_FORECAST_METHOD",
        ]
    ]


def _safe_allocate_monthly_to_pi(
    monthly_df: pd.DataFrame,
    pi_calendar_df: pd.DataFrame,
    *,
    debug: bool = False,
    slice_keys: Optional[dict[str, Any]] = None,
    extra_group_cols: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Allocate monthly PROGRAM-level amounts to PI with validation and diagnostics.

    This prevents many-to-many duplication by enforcing:
    - monthly uniqueness on (PROGRAMID/PROGRAMNAME, YEAR, MONTH_KEY)
    - calendar uniqueness on (YEAR, PI_NAME)
    - weights sum to ~1 per monthly row (renormalized when needed)
    - merge apply step is validate='one_to_many'
    """
    if monthly_df is None or monthly_df.empty:
        program_col = "PROGRAMID" if monthly_df is not None and "PROGRAMID" in monthly_df.columns else "PROGRAMNAME"
        cols = [program_col, "YEAR", "PI_NAME"] + [c for c in (extra_group_cols or []) if c] + ["AMOUNT_PI"]
        return pd.DataFrame(columns=cols)
    if pi_calendar_df is None or pi_calendar_df.empty:
        program_col = "PROGRAMID" if "PROGRAMID" in monthly_df.columns else ("PROGRAMNAME" if "PROGRAMNAME" in monthly_df.columns else "PROGRAM")
        cols = [program_col, "YEAR", "PI_NAME"] + [c for c in (extra_group_cols or []) if c] + ["AMOUNT_PI"]
        return pd.DataFrame(columns=cols)

    program_col = "PROGRAMID" if "PROGRAMID" in monthly_df.columns else ("PROGRAMNAME" if "PROGRAMNAME" in monthly_df.columns else "")
    if not program_col:
        raise ValueError("monthly_df must include PROGRAMID or PROGRAMNAME")

    m = monthly_df.copy()
    m[program_col] = m.get(program_col, "").fillna("").astype(str).str.strip()
    if "PROGRAMNAME" in m.columns:
        m["PROGRAMNAME"] = m.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    m["YEAR"] = _to_int_series_compat(m.get("YEAR"))
    mk = m.get("MONTH_KEY", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    mk = mk.str.replace(r"^(\d{4})(\d{2})$", r"\1-\2", regex=True)
    m["MONTH_KEY"] = mk
    m["AMOUNT"] = pd.to_numeric(m.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
    m = m[m["YEAR"].notna() & (m[program_col] != "") & (m["MONTH_KEY"] != "")].copy()
    if m.empty:
        cols = [program_col, "YEAR", "PI_NAME"] + [c for c in (extra_group_cols or []) if c] + ["AMOUNT_PI"]
        return pd.DataFrame(columns=cols)

    extra_cols = [c for c in (extra_group_cols or []) if c and c in m.columns and c not in (program_col, "YEAR", "MONTH_KEY")]
    for c in extra_cols:
        m[c] = m.get(c, "").fillna("").astype(str).str.strip()
    monthly_keys = [program_col, "YEAR", "MONTH_KEY"] + extra_cols
    dup = m.duplicated(subset=monthly_keys, keep=False)
    if dup.any():
        sample = m.loc[dup, monthly_keys + ["AMOUNT"]].head(10)
        msg = f"Monthly NWF duplicates detected; aggregating. sample={sample.to_dict(orient='records')}"
        logger.warning(msg)
        if debug:
            print(msg)
        if program_col == "PROGRAMNAME":
            m = m.groupby(monthly_keys, dropna=False, as_index=False)["AMOUNT"].sum()
        else:
            agg = m.groupby(monthly_keys, dropna=False, as_index=False).agg(
                {"AMOUNT": "sum", **({"PROGRAMNAME": "first"} if "PROGRAMNAME" in m.columns else {})}
            )
            m = agg.copy()

    cal = pi_calendar_df.copy()
    pi_name_col = "PI_NAME" if "PI_NAME" in cal.columns else ("PI" if "PI" in cal.columns else "")
    if not pi_name_col:
        raise ValueError("pi_calendar_df must include PI_NAME (or PI)")
    cal["YEAR"] = _to_int_series_compat(cal.get("YEAR"))
    cal["PI_NAME"] = cal.get(pi_name_col, "").fillna("").astype(str).str.strip()
    cal["START_DATE"] = pd.to_datetime(cal.get("START_DATE"), errors="coerce").dt.normalize()
    cal["END_DATE"] = pd.to_datetime(cal.get("END_DATE"), errors="coerce").dt.normalize()
    cal = cal[cal["YEAR"].notna() & (cal["PI_NAME"] != "") & cal["START_DATE"].notna() & cal["END_DATE"].notna()].copy()
    if cal.empty:
        return pd.DataFrame(columns=[program_col, "YEAR", "PI_NAME", "AMOUNT_PI"])

    cal_dupe = cal.duplicated(subset=["YEAR", "PI_NAME"], keep=False)
    if cal_dupe.any():
        sample = cal.loc[cal_dupe, ["YEAR", "PI_NAME", "START_DATE", "END_DATE"]].head(10)
        msg = f"PI calendar duplicates detected; collapsing YEAR×PI_NAME. sample={sample.to_dict(orient='records')}"
        logger.warning(msg)
        if debug:
            print(msg)
        cal = cal.groupby(["YEAR", "PI_NAME"], dropna=False, as_index=False).agg({"START_DATE": "min", "END_DATE": "max"})

    # Month dates for overlap.
    m["MONTH_START_DATE"] = pd.to_datetime(m["MONTH_KEY"].str.slice(0, 7) + "-01", errors="coerce").dt.normalize()
    m["MONTH_END_DATE"] = (m["MONTH_START_DATE"] + pd.offsets.MonthEnd(0)).dt.normalize()
    m["MONTH_DAYS"] = (m["MONTH_END_DATE"] - m["MONTH_START_DATE"]).dt.days + 1
    m = m[m["MONTH_START_DATE"].notna() & m["MONTH_END_DATE"].notna() & (m["MONTH_DAYS"] > 0)].copy()
    if m.empty:
        cols = [program_col, "YEAR", "PI_NAME"] + extra_cols + ["AMOUNT_PI"]
        return pd.DataFrame(columns=cols)

    weights = m[monthly_keys + ["MONTH_START_DATE", "MONTH_END_DATE", "MONTH_DAYS"]].merge(
        cal[["YEAR", "PI_NAME", "START_DATE", "END_DATE"]],
        on="YEAR",
        how="left",
        validate="many_to_many",
    )
    weights["OVERLAP_START"] = weights[["MONTH_START_DATE", "START_DATE"]].max(axis=1)
    weights["OVERLAP_END"] = weights[["MONTH_END_DATE", "END_DATE"]].min(axis=1)
    weights["OVERLAP_DAYS"] = (weights["OVERLAP_END"] - weights["OVERLAP_START"]).dt.days + 1
    weights = weights[weights["OVERLAP_DAYS"] > 0].copy()

    if weights.empty:
        msg = "No month↔PI overlaps found; allocating all amounts to '(No PI)'."
        logger.warning(msg)
        if debug:
            print(msg)
        out0 = (
            m.groupby([program_col, "YEAR"] + extra_cols, dropna=False, as_index=False)["AMOUNT"]
            .sum()
            .rename(columns={"AMOUNT": "AMOUNT_PI"})
        )
        out0["PI_NAME"] = "(No PI)"
        out0["AMOUNT_PI"] = pd.to_numeric(out0.get("AMOUNT_PI"), errors="coerce").fillna(0.0).astype(float)
        return out0[[program_col, "YEAR", "PI_NAME"] + extra_cols + ["AMOUNT_PI"]]

    weights["WEIGHT"] = pd.to_numeric(weights["OVERLAP_DAYS"], errors="coerce").fillna(0.0) / pd.to_numeric(
        weights["MONTH_DAYS"], errors="coerce"
    ).fillna(0.0).replace({0.0: pd.NA})
    weights["WEIGHT"] = pd.to_numeric(weights["WEIGHT"], errors="coerce").fillna(0.0).clip(lower=0.0)
    weights = weights[monthly_keys + ["PI_NAME", "WEIGHT"]].copy()
    weights = weights.groupby(monthly_keys + ["PI_NAME"], dropna=False, as_index=False)["WEIGHT"].sum()

    wsum = weights.groupby(monthly_keys, dropna=False)["WEIGHT"].sum().reset_index().rename(columns={"WEIGHT": "WEIGHT_SUM"})

    # If a monthly row has no overlapping PI, create a placeholder weight so totals reconcile.
    base_keys = m[monthly_keys].drop_duplicates()
    wsum_full = base_keys.merge(wsum, on=monthly_keys, how="left").fillna({"WEIGHT_SUM": 0.0})
    missing = wsum_full[pd.to_numeric(wsum_full["WEIGHT_SUM"], errors="coerce").fillna(0.0) <= 0.0].copy()
    if not missing.empty:
        msg = f"Monthly→PI weights missing for some months; allocating to '(No PI)'. months={missing.head(10).to_dict(orient='records')}"
        logger.warning(msg)
        if debug:
            print(msg)
        miss = missing[monthly_keys].copy()
        miss["PI_NAME"] = "(No PI)"
        miss["WEIGHT"] = 1.0
        weights = pd.concat([weights, miss], ignore_index=True, sort=False)
        wsum = weights.groupby(monthly_keys, dropna=False)["WEIGHT"].sum().reset_index().rename(columns={"WEIGHT": "WEIGHT_SUM"})

    offenders = wsum[pd.to_numeric(wsum["WEIGHT_SUM"], errors="coerce").fillna(0.0).sub(1.0).abs() > 0.01].copy()
    if not offenders.empty:
        msg = f"Monthly→PI weights not summing to 1; renormalizing. offenders={offenders.head(10).to_dict(orient='records')}"
        logger.warning(msg)
        if debug:
            print(msg)
    weights = weights.merge(wsum, on=monthly_keys, how="left", validate="many_to_one")
    weights["WEIGHT_SUM"] = pd.to_numeric(weights.get("WEIGHT_SUM"), errors="coerce").fillna(0.0)
    ok = weights["WEIGHT_SUM"] > 0
    weights.loc[ok, "WEIGHT"] = (weights.loc[ok, "WEIGHT"] / weights.loc[ok, "WEIGHT_SUM"]).clip(0.0, 1.0)
    weights = weights.drop(columns=["WEIGHT_SUM"])

    monthly_base = m[monthly_keys + (["PROGRAMNAME"] if "PROGRAMNAME" in m.columns else []) + ["AMOUNT"]].copy()
    alloc = monthly_base.merge(weights, on=monthly_keys, how="left", validate="one_to_many")
    alloc["WEIGHT"] = pd.to_numeric(alloc.get("WEIGHT"), errors="coerce").fillna(0.0)
    alloc["ALLOC_AMOUNT"] = pd.to_numeric(alloc.get("AMOUNT"), errors="coerce").fillna(0.0) * alloc["WEIGHT"]
    alloc = alloc[alloc["WEIGHT"] > 0].copy()

    out_group_keys = [program_col, "YEAR", "PI_NAME"] + extra_cols
    out = alloc.groupby(out_group_keys, dropna=False)["ALLOC_AMOUNT"].sum().reset_index().rename(columns={"ALLOC_AMOUNT": "AMOUNT_PI"})
    out["AMOUNT_PI"] = pd.to_numeric(out.get("AMOUNT_PI"), errors="coerce").fillna(0.0).round(2).astype(float)

    # Reconciliation diagnostics per PROGRAM×YEAR.
    recon_keys = [program_col, "YEAR"] + extra_cols
    monthly_tot = monthly_base.groupby(recon_keys, dropna=False)["AMOUNT"].sum().reset_index().rename(columns={"AMOUNT": "AMOUNT_MONTHLY"})
    pi_tot = out.groupby(recon_keys, dropna=False)["AMOUNT_PI"].sum().reset_index().rename(columns={"AMOUNT_PI": "AMOUNT_PI_TOTAL"})
    recon = monthly_tot.merge(pi_tot, on=recon_keys, how="left").fillna({"AMOUNT_PI_TOTAL": 0.0})
    recon["DELTA"] = pd.to_numeric(recon["AMOUNT_PI_TOTAL"], errors="coerce").fillna(0.0) - pd.to_numeric(
        recon["AMOUNT_MONTHLY"], errors="coerce"
    ).fillna(0.0)

    for _, r in recon.iterrows():
        total = float(r.get("AMOUNT_MONTHLY") or 0.0)
        delta = float(r.get("DELTA") or 0.0)
        tol = max(0.01 * abs(total), 0.05)
        if abs(delta) > tol:
            payload: dict[str, Any] = {
                program_col: r.get(program_col),
                "YEAR": int(r.get("YEAR")) if pd.notna(r.get("YEAR")) else None,
                "AMOUNT_MONTHLY": total,
                "AMOUNT_PI_TOTAL": float(r.get("AMOUNT_PI_TOTAL") or 0.0),
                "DELTA": delta,
                "TOLERANCE": tol,
            }
            for c in extra_cols:
                try:
                    payload[c] = str(r.get(c) or "").strip() or None
                except Exception:
                    payload[c] = None
            for k in ("COST_CATEGORY", "COST_TYPE"):
                if k in monthly_df.columns:
                    try:
                        payload[k] = str(monthly_df.iloc[0].get(k) or "").strip() or None
                    except Exception:
                        payload[k] = None
            if slice_keys:
                payload.update({k: v for k, v in slice_keys.items() if v is not None})
            logger.warning("Monthly→PI allocation reconciliation mismatch: %s", payload)
            if debug:
                print("Monthly→PI allocation reconciliation mismatch:", payload)
                key_val = payload.get(program_col)
                y_val = payload.get("YEAR")
                m_slice = monthly_base[
                    (monthly_base[program_col] == key_val) & (_to_int_series_compat(monthly_base["YEAR"]) == y_val)
                ].copy()
                a_slice = out[(out[program_col] == key_val) & (_to_int_series_compat(out["YEAR"]) == y_val)].copy()
                print("monthly_base sample:", m_slice.sort_values(["MONTH_KEY"]).head(10).to_dict(orient="records"))
                print("allocated_pi sample:", a_slice.sort_values(["PI_NAME"]).head(10).to_dict(orient="records"))
                try:
                    wsum_slice = wsum[
                        (wsum[program_col] == key_val) & (_to_int_series_compat(wsum["YEAR"]) == y_val)
                    ].copy()
                    if not wsum_slice.empty:
                        print("weights_sum sample:", wsum_slice.sort_values(["MONTH_KEY"]).head(10).to_dict(orient="records"))
                except Exception:
                    pass

    return out[[program_col, "YEAR", "PI_NAME"] + extra_cols + ["AMOUNT_PI"]]


def get_program_nwf_actuals_monthly_breakdown(
    db: Any,
    years: list[int],
    filters: Optional[dict] = None,
) -> pd.DataFrame:
    """Return Apptio NWF actuals aggregated at PROGRAM×YEAR×MONTH×COST_TYPE×SUBTYPE (monthly)."""
    fetch = _resolve_fetch(db)
    years0 = sorted({int(y) for y in (years or []) if str(y).strip()})
    if not years0:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH_KEY", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS", "AMOUNT"])

    where: list[str] = ["1=1"]
    params: list[Any] = []
    _add_in(where, params, "a.FISCAL_YEAR", years0)

    programs = [str(p).strip() for p in _as_list((filters or {}).get("program")) if str(p).strip()]
    _add_in(where, params, "p.PROGRAMNAME", programs, upper=True)

    # Prefer the shared DB-layer implementation (single source of truth for mapping precedence/enforcement)
    # when the DB facade exposes it. This avoids query drift between layers.
    try:
        fetch_breakdown = getattr(db, "fetch_apptio_actuals_by_program_breakdown", None)
        if callable(fetch_breakdown):
            program_ids: Optional[list[str]] = None
            if programs:
                try:
                    placeholders = ", ".join(["%s"] * len(programs))
                    ids = fetch(
                        f"""
                        SELECT PROGRAMID
                        FROM PROGRAMS
                        WHERE UPPER(LTRIM(RTRIM(COALESCE(NULLIF(PROGRAM_DISPLAY_NAME, ''), PROGRAMNAME)))) IN ({placeholders})
                        """,
                        [str(p).strip().upper() for p in programs],
                    )
                    if ids is not None and not ids.empty and "PROGRAMID" in ids.columns:
                        program_ids = [str(x).strip() for x in ids["PROGRAMID"].dropna().astype(str).tolist() if str(x).strip()]
                except Exception:
                    program_ids = None

            frames: list[pd.DataFrame] = []
            for y in years0:
                frames.append(fetch_breakdown(fiscal_year=int(y), program_ids=program_ids))  # type: ignore[misc]
            out = pd.concat([f for f in frames if f is not None and not f.empty], ignore_index=True, sort=False) if frames else pd.DataFrame()
            if out is None or out.empty:
                return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH_KEY", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS", "AMOUNT"])
            w = out.copy()
            w["PROGRAMID"] = w.get("PROGRAMID", "").fillna("").astype(str).str.strip()
            w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
            w["YEAR"] = _to_int_series_compat(w.get("YEAR"))
            w["MONTH_KEY"] = w.get("MONTH_KEY", "").fillna("").astype(str).str.strip()
            for c in ("COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS"):
                if c in w.columns:
                    w[c] = w.get(c, "").fillna("").astype(str).str.strip()
            w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
            if programs and "PROGRAMNAME" in w.columns and not program_ids:
                prog_set = {str(p).strip().upper() for p in programs}
                w = w[w["PROGRAMNAME"].astype(str).str.upper().isin(prog_set)].copy()
            w = w[w["YEAR"].notna() & (w["MONTH_KEY"] != "") & (w["PROGRAMNAME"] != "")].copy()
            return w[["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH_KEY", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS", "AMOUNT"]].copy()
    except Exception:
        pass

    # IMPORTANT: enforce a closed-set NWF taxonomy at query-time so Actuals never emit arbitrary labels,
    # and ensure product mappings defaulted to NWF Other do not override ledger fallback mappings.
    sql = f"""
      WITH AllowedPairs AS (
        SELECT 'Invoices' AS COST_TYPE, '' AS SUBTYPE
        UNION ALL SELECT 'Cloud', 'AWS'
        UNION ALL SELECT 'Cloud', 'Azure'
        UNION ALL SELECT 'Contractor CS', ''
        UNION ALL SELECT 'MSP', ''
        UNION ALL SELECT 'Travel', ''
        UNION ALL SELECT 'Infra', ''
        UNION ALL SELECT 'NWF', 'Other'
      ),
      base AS (
        SELECT
          p.PROGRAMID,
          p.PROGRAMNAME,
          TRY_CONVERT(INT, a.FISCAL_YEAR) AS YEAR,
          CONCAT(
            TRY_CONVERT(INT, a.FISCAL_YEAR),
            '-',
            RIGHT('0' + CONVERT(VARCHAR(2), TRY_CONVERT(INT, a.MONTH)), 2)
          ) AS MONTH_KEY,
          map2.COST_TYPE AS MAP_COST_TYPE,
          COALESCE(map2.SUBTYPE, '') AS MAP_SUBTYPE,
          COALESCE(map2.STATUS, '') AS MAP_STATUS,
          CAST(SUM(COALESCE(TRY_CONVERT(FLOAT, a.AMOUNT), 0.0)) AS DECIMAL(18,2)) AS AMOUNT
        FROM APPTIO_ACTUALS_LINES a
        JOIN PROGRAM_APPTIO_WORKIDS w ON w.WORK_ID = a.WORK_ID
        JOIN PROGRAMS p ON p.PROGRAMID = w.PROGRAMID
        OUTER APPLY (
          SELECT TOP 1
            LTRIM(RTRIM(m.COST_TYPE)) AS COST_TYPE,
            COALESCE(LTRIM(RTRIM(m.SUBTYPE)), '') AS SUBTYPE
          FROM MAP_APPTIO_TO_COST_TYPE m
          WHERE m.IS_ACTIVE = 1
            AND (m.FISCAL_YEAR IS NULL OR m.FISCAL_YEAR = a.FISCAL_YEAR)
            AND m.PRODUCT_ID IS NOT NULL
            AND LTRIM(RTRIM(m.PRODUCT_ID)) <> ''
            AND LTRIM(RTRIM(m.PRODUCT_ID)) = LTRIM(RTRIM(COALESCE(a.PRODUCT_ID, '')))
            AND m.COST_TYPE IS NOT NULL
            AND LTRIM(RTRIM(m.COST_TYPE)) <> ''
            AND EXISTS (
              SELECT 1
              FROM AllowedPairs ap2
              WHERE ap2.COST_TYPE = LTRIM(RTRIM(m.COST_TYPE))
                AND ap2.SUBTYPE = COALESCE(LTRIM(RTRIM(m.SUBTYPE)), '')
            )
            AND NOT (
              UPPER(LTRIM(RTRIM(m.COST_TYPE))) = 'NWF'
              AND UPPER(COALESCE(NULLIF(LTRIM(RTRIM(m.SUBTYPE)), ''), 'Other')) = 'OTHER'
            )
          ORDER BY
            CASE WHEN m.FISCAL_YEAR IS NULL THEN 1 ELSE 0 END ASC,
            LEN(COALESCE(m.LEDGER_ACCOUNT_CONTAINS, '')) DESC
        ) product_map
        OUTER APPLY (
          SELECT TOP 1
            LTRIM(RTRIM(m.COST_TYPE)) AS COST_TYPE,
            COALESCE(LTRIM(RTRIM(m.SUBTYPE)), '') AS SUBTYPE
          FROM MAP_APPTIO_TO_COST_TYPE m
          WHERE m.IS_ACTIVE = 1
            AND (m.FISCAL_YEAR IS NULL OR m.FISCAL_YEAR = a.FISCAL_YEAR)
            AND m.PRODUCT_ID IS NULL
            AND m.LEDGER_ACCOUNT_CONTAINS IS NOT NULL
            AND LTRIM(RTRIM(m.LEDGER_ACCOUNT_CONTAINS)) <> ''
            AND UPPER(COALESCE(a.LEDGER_ACCOUNT_L3_DESC, '')) LIKE '%' + UPPER(m.LEDGER_ACCOUNT_CONTAINS) + '%'
          ORDER BY
            CASE WHEN m.FISCAL_YEAR IS NULL THEN 1 ELSE 0 END ASC,
            LEN(COALESCE(m.LEDGER_ACCOUNT_CONTAINS, '')) DESC
        ) ledger_map
        OUTER APPLY (
          SELECT
            COALESCE(product_map.COST_TYPE, ledger_map.COST_TYPE) AS COST_TYPE,
            COALESCE(product_map.SUBTYPE, ledger_map.SUBTYPE, '') AS SUBTYPE,
            CASE
              WHEN product_map.COST_TYPE IS NOT NULL THEN 'MAPPED_PRODUCT'
              WHEN ledger_map.COST_TYPE IS NOT NULL THEN 'MAPPED_LEDGER'
              ELSE ''
            END AS STATUS
        ) map2
        WHERE {" AND ".join(where)}
        GROUP BY
          p.PROGRAMID,
          p.PROGRAMNAME,
          TRY_CONVERT(INT, a.FISCAL_YEAR),
          TRY_CONVERT(INT, a.MONTH),
          map2.COST_TYPE,
          COALESCE(map2.SUBTYPE, ''),
          COALESCE(map2.STATUS, '')
      )
      SELECT
        b.PROGRAMID,
        b.PROGRAMNAME,
        b.YEAR,
        b.MONTH_KEY,
        CASE
          WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'NWF'
          WHEN ap.COST_TYPE IS NULL THEN 'NWF'
          ELSE b.MAP_COST_TYPE
        END AS COST_TYPE,
        CASE
          WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'Other'
          WHEN ap.COST_TYPE IS NULL THEN 'Other'
          ELSE b.MAP_SUBTYPE
        END AS SUBTYPE,
        CASE
          WHEN (
            CASE
              WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'NWF'
              WHEN ap.COST_TYPE IS NULL THEN 'NWF'
              ELSE b.MAP_COST_TYPE
            END
          ) = 'Cloud'
           AND (
            CASE
              WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'Other'
              WHEN ap.COST_TYPE IS NULL THEN 'Other'
              ELSE b.MAP_SUBTYPE
            END
          ) = 'AWS'
            THEN 'Cloud AWS'
          WHEN (
            CASE
              WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'NWF'
              WHEN ap.COST_TYPE IS NULL THEN 'NWF'
              ELSE b.MAP_COST_TYPE
            END
          ) = 'Cloud'
           AND (
            CASE
              WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'Other'
              WHEN ap.COST_TYPE IS NULL THEN 'Other'
              ELSE b.MAP_SUBTYPE
            END
          ) = 'Azure'
            THEN 'Cloud Azure'
          WHEN (
            CASE
              WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'NWF'
              WHEN ap.COST_TYPE IS NULL THEN 'NWF'
              ELSE b.MAP_COST_TYPE
            END
          ) IN ('Invoices','Contractor CS','MSP','Travel','Infra')
            THEN (
              CASE
                WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'NWF'
                WHEN ap.COST_TYPE IS NULL THEN 'NWF'
                ELSE b.MAP_COST_TYPE
              END
            )
          WHEN (
            CASE
              WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'NWF'
              WHEN ap.COST_TYPE IS NULL THEN 'NWF'
              ELSE b.MAP_COST_TYPE
            END
          ) = 'NWF'
           AND (
            CASE
              WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'Other'
              WHEN ap.COST_TYPE IS NULL THEN 'Other'
              ELSE b.MAP_SUBTYPE
            END
          ) IN ('Other','')
            THEN 'NWF Other'
          ELSE 'NWF Other'
        END AS EFFECTIVE_NWF_TYPE,
        CASE
          WHEN b.MAP_COST_TYPE IS NULL OR LTRIM(RTRIM(b.MAP_COST_TYPE)) = '' THEN 'DEFAULT_OTHER'
          WHEN ap.COST_TYPE IS NULL THEN 'INVALID_TARGET'
          ELSE b.MAP_STATUS
        END AS MAPPING_STATUS,
        b.AMOUNT
      FROM base b
      LEFT JOIN AllowedPairs ap
        ON ap.COST_TYPE = b.MAP_COST_TYPE
       AND ap.SUBTYPE = b.MAP_SUBTYPE
      ORDER BY YEAR, MONTH_KEY, PROGRAMNAME, COST_TYPE, SUBTYPE
    """
    out = fetch(sql, params)
    if out is None or out.empty:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH_KEY", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS", "AMOUNT"])
    w = out.copy()
    w["PROGRAMID"] = w.get("PROGRAMID", "").fillna("").astype(str).str.strip()
    w["PROGRAMNAME"] = w.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    w["YEAR"] = _to_int_series_compat(w.get("YEAR"))
    w["MONTH_KEY"] = w.get("MONTH_KEY", "").fillna("").astype(str).str.strip()
    w["COST_TYPE"] = w.get("COST_TYPE", "").fillna("").astype(str).str.strip()
    w["SUBTYPE"] = w.get("SUBTYPE", "").fillna("").astype(str).str.strip()
    w["EFFECTIVE_NWF_TYPE"] = w.get("EFFECTIVE_NWF_TYPE", "").fillna("").astype(str).str.strip()
    w["MAPPING_STATUS"] = w.get("MAPPING_STATUS", "").fillna("").astype(str).str.strip()
    w["AMOUNT"] = pd.to_numeric(w.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
    w = w[w["YEAR"].notna() & (w["MONTH_KEY"] != "") & (w["PROGRAMNAME"] != "")].copy()
    return w[["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH_KEY", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS", "AMOUNT"]]


def get_program_nwf_actuals_by_pi_breakdown(db: Any, years: list[int], filters: Optional[dict] = None) -> pd.DataFrame:
    monthly = get_program_nwf_actuals_monthly_breakdown(db, years=years, filters=filters)
    if monthly is None or monthly.empty:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS", "AMOUNT_PI"])
    years0 = sorted({int(y) for y in pd.to_numeric(monthly.get("YEAR"), errors="coerce").dropna().astype(int).tolist()})
    cal_parts: list[pd.DataFrame] = []
    for y in years0:
        root = _resolve_pi_calendar_root(db, year=int(y), filters=filters)
        cal_parts.append(load_pi_calendar(db, years=[int(y)], calendar_root=root))
    cal = pd.concat([c for c in cal_parts if c is not None and not c.empty], ignore_index=True, sort=False) if cal_parts else pd.DataFrame()
    debug = bool((filters or {}).get("debug_nwf_allocation") or (filters or {}).get("debug"))
    if cal is None or cal.empty:
        m = monthly.copy()
        m["YEAR"] = _to_int_series_compat(m.get("YEAR"))
        month_num = pd.to_numeric(m.get("MONTH_KEY", "").astype(str).str[-2:], errors="coerce")
        m["PI_NUM"] = ((month_num - 1) // 3 + 1).where(month_num.between(1, 12), other=pd.NA)
        m = m[m["YEAR"].notna() & m["PI_NUM"].notna()].copy()
        if m.empty:
            return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS", "AMOUNT_PI"])
        m["PI_NAME"] = m["YEAR"].astype(int).astype(str) + " I" + m["PI_NUM"].astype(int).astype(str)
        out = (
            m.groupby(["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS"], dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "AMOUNT_PI"})
        )
        out["AMOUNT_PI"] = pd.to_numeric(out.get("AMOUNT_PI"), errors="coerce").fillna(0.0).astype(float)
        return out[["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS", "AMOUNT_PI"]]

    monthly_for_alloc = monthly[
        [
            "PROGRAMID",
            "PROGRAMNAME",
            "YEAR",
            "MONTH_KEY",
            "AMOUNT",
            "COST_TYPE",
            "SUBTYPE",
            "EFFECTIVE_NWF_TYPE",
            "MAPPING_STATUS",
        ]
    ].copy()
    alloc = _safe_allocate_monthly_to_pi(
        monthly_for_alloc,
        cal,
        debug=debug,
        slice_keys={"SCENARIO": "Actual"},
        extra_group_cols=["COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS"],
    )
    if alloc is None or alloc.empty:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS", "AMOUNT_PI"])
    name_map = monthly[["PROGRAMID", "PROGRAMNAME", "YEAR", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS"]].drop_duplicates()
    out = alloc.merge(name_map, on=["PROGRAMID", "YEAR", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS"], how="left")
    out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    out["PROGRAMID"] = out.get("PROGRAMID", "").fillna("").astype(str).str.strip()
    out["YEAR"] = _to_int_series_compat(out.get("YEAR"))
    out["PI_NAME"] = out.get("PI_NAME", "").fillna("").astype(str).str.strip()
    out["AMOUNT_PI"] = pd.to_numeric(out.get("AMOUNT_PI"), errors="coerce").fillna(0.0).astype(float)
    out["COST_TYPE"] = out.get("COST_TYPE", "").fillna("").astype(str).str.strip()
    out["SUBTYPE"] = out.get("SUBTYPE", "").fillna("").astype(str).str.strip()
    out["EFFECTIVE_NWF_TYPE"] = out.get("EFFECTIVE_NWF_TYPE", "").fillna("").astype(str).str.strip()
    out["MAPPING_STATUS"] = out.get("MAPPING_STATUS", "").fillna("").astype(str).str.strip()
    return out[["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "COST_TYPE", "SUBTYPE", "EFFECTIVE_NWF_TYPE", "MAPPING_STATUS", "AMOUNT_PI"]]


def get_program_nwf_actuals_by_pi(db: Any, years: list[int], filters: Optional[dict] = None) -> pd.DataFrame:
    monthly = get_program_nwf_actuals_monthly(db, years=years, filters=filters)
    if monthly is None or monthly.empty:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "AMOUNT_PI"])
    years0 = sorted({int(y) for y in pd.to_numeric(monthly.get("YEAR"), errors="coerce").dropna().astype(int).tolist()})
    cal_parts: list[pd.DataFrame] = []
    for y in years0:
        root = _resolve_pi_calendar_root(db, year=int(y), filters=filters)
        cal_parts.append(load_pi_calendar(db, years=[int(y)], calendar_root=root))
    cal = pd.concat([c for c in cal_parts if c is not None and not c.empty], ignore_index=True, sort=False) if cal_parts else pd.DataFrame()
    debug = bool((filters or {}).get("debug_nwf_allocation") or (filters or {}).get("debug"))
    if cal is None or cal.empty:
        # Fallback: map months to PIs by quarter when the PI calendar is unavailable.
        m = monthly.copy()
        m["YEAR"] = _to_int_series_compat(m.get("YEAR"))
        month_num = pd.to_numeric(m.get("MONTH_KEY", "").astype(str).str[-2:], errors="coerce")
        m["PI_NUM"] = ((month_num - 1) // 3 + 1).where(month_num.between(1, 12), other=pd.NA)
        m = m[m["YEAR"].notna() & m["PI_NUM"].notna()].copy()
        if m.empty:
            return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "AMOUNT_PI"])
        m["PI_NAME"] = m["YEAR"].astype(int).astype(str) + " I" + m["PI_NUM"].astype(int).astype(str)
        m["AMOUNT"] = pd.to_numeric(m.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)
        if debug:
            print("NWF actuals: PI calendar empty; allocating by month->quarter fallback.")
        out = (
            m.groupby(["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME"], dropna=False)["AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"AMOUNT": "AMOUNT_PI"})
        )
        out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
        out["PROGRAMID"] = out.get("PROGRAMID", "").fillna("").astype(str).str.strip()
        out["YEAR"] = _to_int_series_compat(out.get("YEAR"))
        out["PI_NAME"] = out.get("PI_NAME", "").fillna("").astype(str).str.strip()
        out["AMOUNT_PI"] = pd.to_numeric(out.get("AMOUNT_PI"), errors="coerce").fillna(0.0).astype(float)
        return out[["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "AMOUNT_PI"]]
    alloc = _safe_allocate_monthly_to_pi(monthly, cal, debug=debug, slice_keys={"SCENARIO": "Actual"})
    if alloc is None or alloc.empty:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "AMOUNT_PI"])
    out = alloc.merge(monthly[["PROGRAMID", "PROGRAMNAME", "YEAR"]].drop_duplicates(), on=["PROGRAMID", "YEAR"], how="left")
    out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    out["PROGRAMID"] = out.get("PROGRAMID", "").fillna("").astype(str).str.strip()
    out["YEAR"] = _to_int_series_compat(out.get("YEAR"))
    out["PI_NAME"] = out.get("PI_NAME", "").fillna("").astype(str).str.strip()
    out["AMOUNT_PI"] = pd.to_numeric(out.get("AMOUNT_PI"), errors="coerce").fillna(0.0).astype(float)
    return out[["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "AMOUNT_PI"]]


def get_program_nwf_expected_by_pi(db: Any, years: list[int], filters: Optional[dict] = None) -> pd.DataFrame:
    monthly_expected = get_program_nwf_expected_monthly(db, years=years, filters=filters)
    if monthly_expected is None or monthly_expected.empty:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "AMOUNT_PI"])
    monthly_for_alloc = monthly_expected.rename(columns={"EXPECTED_AMOUNT": "AMOUNT"})[
        ["PROGRAMID", "PROGRAMNAME", "YEAR", "MONTH_KEY", "AMOUNT"]
    ].copy()
    years0 = sorted({int(y) for y in pd.to_numeric(monthly_for_alloc.get("YEAR"), errors="coerce").dropna().astype(int).tolist()})
    cal_parts: list[pd.DataFrame] = []
    for y in years0:
        root = _resolve_pi_calendar_root(db, year=int(y), filters=filters)
        cal_parts.append(load_pi_calendar(db, years=[int(y)], calendar_root=root))
    cal = pd.concat([c for c in cal_parts if c is not None and not c.empty], ignore_index=True, sort=False) if cal_parts else pd.DataFrame()
    debug = bool((filters or {}).get("debug_nwf_allocation") or (filters or {}).get("debug"))
    alloc = _safe_allocate_monthly_to_pi(monthly_for_alloc, cal, debug=debug, slice_keys={"SCENARIO": "Projected"})
    if alloc is None or alloc.empty:
        return pd.DataFrame(columns=["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "AMOUNT_PI"])
    out = alloc.merge(monthly_for_alloc[["PROGRAMID", "PROGRAMNAME", "YEAR"]].drop_duplicates(), on=["PROGRAMID", "YEAR"], how="left")
    out["PROGRAMNAME"] = out.get("PROGRAMNAME", "").fillna("").astype(str).str.strip()
    out["PROGRAMID"] = out.get("PROGRAMID", "").fillna("").astype(str).str.strip()
    out["YEAR"] = _to_int_series_compat(out.get("YEAR"))
    out["PI_NAME"] = out.get("PI_NAME", "").fillna("").astype(str).str.strip()
    out["AMOUNT_PI"] = pd.to_numeric(out.get("AMOUNT_PI"), errors="coerce").fillna(0.0).astype(float)
    return out[["PROGRAMID", "PROGRAMNAME", "YEAR", "PI_NAME", "AMOUNT_PI"]]
