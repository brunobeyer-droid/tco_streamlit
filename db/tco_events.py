from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import pandas as pd

from db import ensure_cost_events_table, execute, fetch_df, _fq
from db.pi_calendar import map_date_to_pi, map_iteration_path_to_pi


def _to_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or (not isinstance(val, str) and pd.isna(val)):
            return float(default)
        return float(val)
    except Exception:
        return float(default)


def _to_int(val: Any, default: int = 0) -> int:
    try:
        if val is None or (not isinstance(val, str) and pd.isna(val)):
            return int(default)
        return int(val)
    except Exception:
        return int(default)


def _s(val: Any) -> str:
    return str(val or "").strip()


def _invoice_pi_from_renewal(renewal_date: Any) -> Optional[int]:
    """Match invoice PI mapping used by `VW_INVOICE_SPEND_PI` (quarter by renewal month)."""
    try:
        if renewal_date is None or (not isinstance(renewal_date, str) and pd.isna(renewal_date)):
            return None
    except Exception:
        return None

    d = renewal_date
    if isinstance(d, str):
        try:
            d = date.fromisoformat(d[:10])
        except Exception:
            return None
    if not isinstance(d, date):
        try:
            d = pd.to_datetime(d).date()
        except Exception:
            return None
    m = int(getattr(d, "month", 0) or 0)
    if 1 <= m <= 3:
        return 1
    if 4 <= m <= 6:
        return 2
    if 7 <= m <= 9:
        return 3
    if 10 <= m <= 12:
        return 4
    return None


def _to_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            try:
                return pd.to_datetime(s).date()
            except Exception:
                return None
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


@dataclass(frozen=True)
class InvoiceScope:
    program: Optional[str]
    team: Optional[str]
    app_group: Optional[str]


def _lookup_invoice_scope(team_id: str, group_id: str, programid_at_booking: str) -> InvoiceScope:
    team_id = _s(team_id)
    group_id = _s(group_id)
    programid_at_booking = _s(programid_at_booking)

    program = None
    team = None
    app_group = None

    try:
        if team_id:
            df_team = fetch_df(
                f"""
                SELECT TOP 1 t.TEAMNAME, p.PROGRAMNAME
                FROM { _fq('TEAMS') } t
                LEFT JOIN { _fq('PROGRAMS') } p ON p.PROGRAMID = t.PROGRAMID
                WHERE t.TEAMID = %s
                """,
                (team_id,),
            )
            if df_team is not None and not df_team.empty:
                team = _s(df_team.iloc[0].get("TEAMNAME"))
                program = _s(df_team.iloc[0].get("PROGRAMNAME")) or program
    except Exception:
        pass

    try:
        if programid_at_booking and not program:
            df_prog = fetch_df(f"SELECT TOP 1 PROGRAMNAME FROM { _fq('PROGRAMS') } WHERE PROGRAMID=%s", (programid_at_booking,))
            if df_prog is not None and not df_prog.empty:
                program = _s(df_prog.iloc[0].get("PROGRAMNAME")) or program
    except Exception:
        pass

    try:
        if group_id:
            df_grp = fetch_df(f"SELECT TOP 1 GROUPNAME FROM { _fq('APPLICATION_GROUPS') } WHERE GROUPID=%s", (group_id,))
            if df_grp is not None and not df_grp.empty:
                app_group = _s(df_grp.iloc[0].get("GROUPNAME")) or app_group
    except Exception:
        pass

    return InvoiceScope(program=program or None, team=team or None, app_group=app_group or None)


def insert_cost_event(
    conn: Any,
    fiscal_year: int,
    pi_name: Optional[str],
    iteration_name: Optional[str],
    program: Optional[str],
    team: Optional[str],
    app_group: Optional[str],
    cost_bucket: str,
    event_type: str,
    amount_delta: float = 0.0,
    headcount_delta: float = 0.0,
    source_system: str = "MANUAL",
    related_object_type: Optional[str] = None,
    related_object_id: Optional[str] = None,
    notes: Optional[str] = None,
    event_date: Optional[date] = None,
) -> None:
    """Insert a row into `TCO_COST_EVENTS`."""
    _ = conn  # connection is managed by `db.execute` in this repo
    try:
        ensure_cost_events_table()
    except Exception:
        pass

    pi = _s(pi_name) or None
    bucket = _s(cost_bucket).upper() or "NWF"
    if bucket not in {"WF", "NWF"}:
        bucket = "NWF"
    ev = _s(event_type).upper() or "MANUAL_ADJUSTMENT"
    src = _s(source_system).upper() or "MANUAL"

    def _params(pi_value: Optional[str]):
        return (
            int(fiscal_year),
            event_date.isoformat() if isinstance(event_date, date) else None,
            pi_value,
            _s(iteration_name) or None,
            _s(program) or None,
            _s(team) or None,
            _s(app_group) or None,
            bucket,
            ev,
            float(_to_float(amount_delta, 0.0)),
            float(_to_float(headcount_delta, 0.0)),
            src,
            _s(related_object_type) or None,
            _s(related_object_id) or None,
            _s(notes) or None,
        )

    sql_with_date = f"""
      INSERT INTO { _fq('TCO_COST_EVENTS') } (
        FISCAL_YEAR,
        EVENT_DATE,
        PI_NAME,
        ITERATION_NAME,
        PROGRAM,
        TEAM,
        APP_GROUP,
        COST_BUCKET,
        EVENT_TYPE,
        AMOUNT_DELTA,
        HEADCOUNT_DELTA,
        SOURCE_SYSTEM,
        RELATED_OBJECT_TYPE,
        RELATED_OBJECT_ID,
        NOTES
      ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
      )
    """
    sql_no_date = f"""
      INSERT INTO { _fq('TCO_COST_EVENTS') } (
        FISCAL_YEAR,
        PI_NAME,
        ITERATION_NAME,
        PROGRAM,
        TEAM,
        APP_GROUP,
        COST_BUCKET,
        EVENT_TYPE,
        AMOUNT_DELTA,
        HEADCOUNT_DELTA,
        SOURCE_SYSTEM,
        RELATED_OBJECT_TYPE,
        RELATED_OBJECT_ID,
        NOTES
      ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
      )
    """

    try:
        execute(sql_with_date, _params(pi))
        return
    except Exception as e:
        msg = str(e).upper()
        if "EVENT_DATE" in msg and ("INVALID COLUMN" in msg or "UNKNOWN COLUMN" in msg):
            try:
                prm = _params(pi)
                execute(sql_no_date, (prm[0], prm[2], prm[3], prm[4], prm[5], prm[6], prm[7], prm[8], prm[9], prm[10], prm[11], prm[12], prm[13], prm[14]))
                return
            except Exception:
                pass
        if pi is None and "PI_NAME" in msg and ("CANNOT INSERT" in msg or "NOT NULL" in msg):
            try:
                execute(sql_with_date, _params("(Unassigned)"))
                return
            except Exception:
                pass
        raise


def record_cost_event_from_invoice(conn: Any, old_invoice: Optional[dict], new_invoice: dict) -> None:
    """Create cost events for invoice creation or updates."""
    _ = conn
    if not isinstance(new_invoice, dict) or not new_invoice:
        return

    inv_id = _s(new_invoice.get("INVOICEID") or new_invoice.get("invoice_id") or new_invoice.get("INVOICE_ID"))
    fiscal_year = _to_int(new_invoice.get("FISCAL_YEAR") or new_invoice.get("fiscal_year"), 0)
    if fiscal_year <= 0:
        return

    pi_name = None
    iteration_name = None

    # Prefer feature-linked mapping when available.
    feature_id = _s(new_invoice.get("FEATURE_ID") or new_invoice.get("FEATUREID") or new_invoice.get("ADO_FEATURE_ID"))
    if feature_id:
        try:
            df_feat = fetch_df(
                f"SELECT TOP 1 ITERATION_PATH FROM { _fq('ADO_FEATURES') } WHERE FEATURE_ID = %s",
                (feature_id,),
            )
            if df_feat is not None and not df_feat.empty:
                it_path = _s(df_feat.iloc[0].get("ITERATION_PATH"))
                if it_path:
                    pi_name, iteration_name = map_iteration_path_to_pi(conn, it_path)
        except Exception:
            pass

    # Fallback: map invoice date to PI via calendar.
    if not pi_name:
        inv_date = new_invoice.get("INVOICEDATE") or new_invoice.get("RENEWALDATE") or new_invoice.get("renewal_date")
        try:
            pi_name, iteration_name = map_date_to_pi(conn, int(fiscal_year), inv_date)  # type: ignore[arg-type]
        except Exception:
            pi_name, iteration_name = None, None
    else:
        inv_date = new_invoice.get("INVOICEDATE") or new_invoice.get("RENEWALDATE") or new_invoice.get("renewal_date")

    # Final fallback: legacy quarter mapping (matches VW_INVOICE_SPEND_PI).
    if not pi_name:
        renewal = new_invoice.get("RENEWALDATE") or new_invoice.get("renewal_date")
        pi_num = _invoice_pi_from_renewal(renewal)
        pi_name = f"I{pi_num}" if pi_num in {1, 2, 3, 4} else "(Unassigned)"

    group_id = _s(new_invoice.get("GROUPID") or "") or _s(new_invoice.get("GROUPID_AT_BOOKING") or "")
    team_id = _s(new_invoice.get("TEAMID") or "")
    programid_at_booking = _s(new_invoice.get("PROGRAMID_AT_BOOKING") or "")
    scope = _lookup_invoice_scope(team_id=team_id, group_id=group_id, programid_at_booking=programid_at_booking)

    service_type = _s(new_invoice.get("SERVICE_TYPE") or "")
    st_up = service_type.upper()
    cost_bucket = "WF" if any(x in st_up for x in ["MSP", "STAFF", "AUG", "OUTSOURC"]) else "NWF"

    new_amt = _to_float(new_invoice.get("AMOUNT") or new_invoice.get("amount"), 0.0)
    if old_invoice is None:
        amount_delta = float(new_amt)
        if amount_delta == 0.0:
            return
        event_type = "ADHOC_INVOICE"
        note = "Invoice created"
    else:
        old_amt = _to_float(old_invoice.get("AMOUNT") or old_invoice.get("amount"), 0.0)
        amount_delta = float(new_amt - old_amt)
        if amount_delta == 0.0:
            return
        event_type = "INVOICE_CHANGE_ORDER"
        note = "Invoice updated"

    insert_cost_event(
        conn,
        fiscal_year=int(fiscal_year),
        pi_name=pi_name,
        iteration_name=iteration_name,
        program=scope.program,
        team=scope.team,
        app_group=scope.app_group,
        cost_bucket=cost_bucket,
        event_type=event_type,
        amount_delta=float(amount_delta),
        headcount_delta=0.0,
        source_system="INVOICE",
        related_object_type="INVOICE",
        related_object_id=inv_id or None,
        notes=f"{note}{' • ' + service_type if service_type else ''}",
        event_date=_to_date(inv_date),
    )


def record_cost_events_from_headcount_delta(
    conn: Any,
    fiscal_year: int,
    pi_name: str,
    program: str,
    team: Optional[str],
    app_group: Optional[str],
    bucket_name: str,
    old_headcount: float,
    new_headcount: float,
    annual_rate: float,
    notes: Optional[str] = None,
    *,
    cost_bucket: str = "WF",
    related_object_id: Optional[str] = None,
) -> None:
    """Create HIRE/TERMINATION events from a headcount delta for a given PI."""
    _ = conn
    delta_hc = float(_to_float(new_headcount, 0.0) - _to_float(old_headcount, 0.0))
    if delta_hc == 0.0:
        return

    rate = float(_to_float(annual_rate, 0.0))
    amount_delta = float(delta_hc * rate)
    event_type = "HIRE" if delta_hc > 0 else "TERMINATION"
    rid = related_object_id or f"{_s(program)}|{_s(team)}|{_s(bucket_name)}|{_s(pi_name)}"
    msg = notes or ""
    if bucket_name:
        msg = (msg + " • " if msg else "") + f"{bucket_name} headcount changed {delta_hc:+.2f}"

    insert_cost_event(
        conn,
        fiscal_year=int(fiscal_year),
        pi_name=_s(pi_name) or "(Unassigned)",
        iteration_name=None,
        program=_s(program) or None,
        team=_s(team) or None,
        app_group=_s(app_group) or None,
        cost_bucket=_s(cost_bucket).upper() or "WF",
        event_type=event_type,
        amount_delta=float(amount_delta),
        headcount_delta=float(delta_hc),
        source_system="WORKFORCE",
        related_object_type="HEADCOUNT",
        related_object_id=_s(rid) or None,
        notes=msg or None,
    )
