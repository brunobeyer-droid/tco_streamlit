# snowflake_db.py
from __future__ import annotations

import os
from typing import Any, Iterable, Optional, Tuple, List, Dict

import pandas as pd
import streamlit as st
import snowflake.connector


# =========================================================
# Config / connection
# =========================================================

def _read_snowflake_secrets() -> dict:
    """Read Snowflake config from st.secrets or environment variables."""
    if "snowflake" in st.secrets:
        cfg = st.secrets["snowflake"]
        return {
            "account":   cfg.get("account"),
            "user":      cfg.get("user"),
            "password":  cfg.get("password"),
            "warehouse": cfg.get("warehouse"),
            "database":  cfg.get("database", "TCODB"),
            "schema":    cfg.get("schema", "PUBLIC"),
            "role":      cfg.get("role"),
        }
    # Fallback to env vars
    return {
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database":  os.getenv("SNOWFLAKE_DATABASE", "TCODB"),
        "schema":    os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "role":      os.getenv("SNOWFLAKE_ROLE"),
    }


def _fq(table: str) -> str:
    """Fully-qualified table name using configured DB/Schema."""
    cfg = _read_snowflake_secrets()
    database = cfg.get("database", "TCODB")
    schema   = cfg.get("schema",   "PUBLIC")
    return f"{database}.{schema}.{table}"


@st.cache_resource(show_spinner=False)
def _get_connection():
    cfg = _read_snowflake_secrets()
    missing = [k for k in ("account","user","password","warehouse") if not cfg.get(k)]
    if missing:
        raise RuntimeError(
            "Snowflake config missing: " + ", ".join(missing) +
            ". Provide .streamlit/secrets.toml [snowflake] or environment variables."
        )
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        password=cfg["password"],
        warehouse=cfg["warehouse"],
        database=cfg.get("database", "TCODB"),
        schema=cfg.get("schema", "PUBLIC"),
        role=cfg.get("role"),
        client_session_keep_alive=True,
    )


# =========================================================
# Low-level helpers
# =========================================================

def execute(sql: str, params: Optional[Iterable[Any]] = None, many: bool = False) -> None:
    """
    Execute a statement. Use many=True with 'params' as list of tuples for executemany.
    Commits after execution.
    """
    conn = _get_connection()
    with conn.cursor() as cur:
        if many and params is not None:
            cur.executemany(sql, params)  # type: ignore[arg-type]
        else:
            cur.execute(sql, params)
    conn.commit()


def fetch_df(sql: str, params: Optional[Iterable[Any]] = None) -> pd.DataFrame:
    """Run a SELECT and return a pandas DataFrame (dict cursor)."""
    conn = _get_connection()
    with conn.cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# =========================================================
# Schema management + guards
# =========================================================

def _db_and_schema() -> Tuple[str, str]:
    """Return (database, schema) for current config/session."""
    try:
        cfg = _read_snowflake_secrets()
        return (cfg.get("database", "TCODB"), cfg.get("schema", "PUBLIC"))
    except Exception:
        row = fetch_df("select current_database() as DB, current_schema() as SCH").iloc[0]
        return row["DB"], row["SCH"]


def _table_has_column(db: str, sch: str, table: str, col: str) -> bool:
    q = f"""
      SELECT 1
      FROM {db}.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND UPPER(COLUMN_NAME) = UPPER(%s)
      LIMIT 1
    """
    df = fetch_df(q, (sch, table.upper(), col.upper()))
    return not df.empty


def _constraint_exists(db: str, sch: str, table: str, constraint_name: str) -> bool:
    q = f"""
      SELECT 1
      FROM {db}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
      WHERE TABLE_SCHEMA = %s
        AND TABLE_NAME = %s
        AND CONSTRAINT_NAME = %s
      LIMIT 1
    """
    df = fetch_df(q, (sch, table.upper(), constraint_name))
    return not df.empty


def _add_unique_if_absent(table: str, columns: str, constraint_name: str) -> None:
    """Idempotently add a UNIQUE constraint; ignore if it already exists or if existing dupes block creation."""
    db, sch = _db_and_schema()
    if _constraint_exists(db, sch, table, constraint_name):
        return
    try:
        execute(f"ALTER TABLE { _fq(table) } ADD CONSTRAINT {constraint_name} UNIQUE ({columns})")
    except Exception:
        # If duplicates exist, Snowflake will reject; we still enforce in app.
        pass


def ensure_groups_teamid() -> None:
    """
    Ensure APPLICATION_GROUPS exists and has TEAMID column; add FK.
    Maintain denormalized PROGRAMID column synced from TEAMS.PROGRAMID.
    """
    db, sch = _db_and_schema()

    # Base table
    execute(f"""
      CREATE TABLE IF NOT EXISTS {db}.{sch}.APPLICATION_GROUPS (
        GROUPID STRING PRIMARY KEY,
        GROUPNAME STRING UNIQUE,
        DEFAULT_VENDORID STRING,
        OWNER STRING,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
      )
    """)

    # TEAMID column + FK to TEAMS
    if not _table_has_column(db, sch, "APPLICATION_GROUPS", "TEAMID"):
        execute(f"ALTER TABLE {db}.{sch}.APPLICATION_GROUPS ADD COLUMN TEAMID STRING")
        try:
            execute(f"""
              ALTER TABLE {db}.{sch}.APPLICATION_GROUPS
              ADD CONSTRAINT FK_GROUP_TEAM FOREIGN KEY (TEAMID)
              REFERENCES {db}.{sch}.TEAMS(TEAMID)
            """)
        except Exception:
            pass

    # Denormalized PROGRAMID
    if not _table_has_column(db, sch, "APPLICATION_GROUPS", "PROGRAMID"):
        execute(f"ALTER TABLE {db}.{sch}.APPLICATION_GROUPS ADD COLUMN PROGRAMID STRING")

    # Sync PROGRAMID from TEAMS
    execute(f"""
      UPDATE {db}.{sch}.APPLICATION_GROUPS g
      SET PROGRAMID = t.PROGRAMID
      FROM {db}.{sch}.TEAMS t
      WHERE g.TEAMID = t.TEAMID
        AND (g.PROGRAMID IS NULL OR g.PROGRAMID <> t.PROGRAMID)
    """)

    # Optional legacy backfill (if TEAMS_APP_GROUPS existed)
    try:
        _ = fetch_df(f"SELECT 1 FROM {db}.{sch}.TEAMS_APP_GROUPS LIMIT 1")
        rows = fetch_df(f"""
          SELECT GROUPID, COUNT(DISTINCT TEAMID) AS CNT, MIN(TEAMID) AS THE_TEAM
          FROM {db}.{sch}.TEAMS_APP_GROUPS
          GROUP BY GROUPID
        """)
        if not rows.empty:
            updates = []
            for _, r in rows.iterrows():
                if int(r["CNT"]) == 1 and r["THE_TEAM"]:
                    updates.append((r["THE_TEAM"], r["GROUPID"]))
            if updates:
                execute(f"""
                  UPDATE {db}.{sch}.APPLICATION_GROUPS
                  SET TEAMID = %s
                  WHERE GROUPID = %s AND TEAMID IS NULL
                """, updates, many=True)
                execute(f"""
                  UPDATE {db}.{sch}.APPLICATION_GROUPS g
                  SET PROGRAMID = t.PROGRAMID
                  FROM {db}.{sch}.TEAMS t
                  WHERE g.TEAMID = t.TEAMID
                    AND (g.PROGRAMID IS NULL OR g.PROGRAMID <> t.PROGRAMID)
                """)
    except Exception:
        pass


def ensure_tables() -> None:
    """Create/patch core tables & constraints; safe to call repeatedly."""
    # PROGRAMS
    execute(f"""
        CREATE TABLE IF NOT EXISTS { _fq("PROGRAMS") } (
            PROGRAMID    STRING PRIMARY KEY,
            PROGRAMNAME  STRING,
            PROGRAMOWNER STRING,
            PROGRAMFTE   FLOAT
        )
    """)
    _add_unique_if_absent("PROGRAMS", "PROGRAMNAME", "UQ_PROGRAMS_PROGRAMNAME")

    # TEAMS
    execute(f"""
        CREATE TABLE IF NOT EXISTS { _fq("TEAMS") } (
            TEAMID       STRING PRIMARY KEY,
            TEAMNAME     STRING,
            PROGRAMID    STRING,
            TEAMFTE      FLOAT,
            COSTPERFTE   FLOAT
        )
    """)
    _add_unique_if_absent("TEAMS", "TEAMNAME", "UQ_TEAMS_TEAMNAME")

    # Ensure numeric columns exist
    execute(f"ALTER TABLE { _fq('TEAMS') } ADD COLUMN IF NOT EXISTS DELIVERY_TEAM_FTE FLOAT")
    execute(f"ALTER TABLE { _fq('TEAMS') } ADD COLUMN IF NOT EXISTS CONTRACTOR_C_FTE FLOAT")
    # NEW NAME: CONTRACTOR_CS_FTE (rename from old if present)
    db, sch = _db_and_schema()
    try:
        if _table_has_column(db, sch, "TEAMS", "CONTRACTOR_CS") and not _table_has_column(db, sch, "TEAMS", "CONTRACTOR_CS_FTE"):
            execute(f"ALTER TABLE {db}.{sch}.TEAMS RENAME COLUMN CONTRACTOR_CS TO CONTRACTOR_CS_FTE")
    except Exception:
        pass
    execute(f"ALTER TABLE { _fq('TEAMS') } ADD COLUMN IF NOT EXISTS CONTRACTOR_CS_FTE FLOAT")

    # NEW: Product Owner on Teams
    execute(f"ALTER TABLE { _fq('TEAMS') } ADD COLUMN IF NOT EXISTS PRODUCTOWNER STRING")

    # VENDORS
    execute(f"""
        CREATE TABLE IF NOT EXISTS { _fq("VENDORS") } (
            VENDORID   STRING PRIMARY KEY,
            VENDORNAME STRING
        )
    """)
    _add_unique_if_absent("VENDORS", "VENDORNAME", "UQ_VENDORS_VENDORNAME")

    # GROUPS + PROGRAMID sync
    ensure_groups_teamid()

    # APPLICATIONS
    execute(f"""
        CREATE TABLE IF NOT EXISTS { _fq("APPLICATIONS") } (
            APPLICATIONID   STRING PRIMARY KEY,
            APPLICATIONNAME STRING,
            VENDORID        STRING,
            GROUPID         STRING,
            ADD_INFO        STRING
        )
    """)
    # Migrate old SITE->ADD_INFO if necessary
    try:
        if _table_has_column(db, sch, "APPLICATIONS", "SITE") and not _table_has_column(db, sch, "APPLICATIONS", "ADD_INFO"):
            execute(f"ALTER TABLE {db}.{sch}.APPLICATIONS RENAME COLUMN SITE TO ADD_INFO")
    except Exception:
        execute(f"ALTER TABLE { _fq('APPLICATIONS') } ADD COLUMN IF NOT EXISTS ADD_INFO STRING")

    # FK + uniqueness
    try:
        execute(f"""
            ALTER TABLE { _fq('APPLICATIONS') }
            ADD CONSTRAINT FK_APP_GROUP FOREIGN KEY (GROUPID)
            REFERENCES { _fq('APPLICATION_GROUPS') }(GROUPID)
        """)
    except Exception:
        pass
    try:
        execute(f"ALTER TABLE { _fq('APPLICATIONS') } DROP CONSTRAINT UQ_APP_GROUP_SITE")
    except Exception:
        pass
    try:
        execute(f"ALTER TABLE { _fq('APPLICATIONS') } ADD CONSTRAINT UQ_APP_GROUP_ADD_INFO UNIQUE (GROUPID, ADD_INFO)")
    except Exception:
        pass
    try:
        execute(f"ALTER TABLE { _fq('APPLICATIONS') } ADD CONSTRAINT UQ_APPLICATIONS_NAME UNIQUE (APPLICATIONNAME)")
    except Exception:
        pass

    # INVOICES (+ extended fields)
    execute(f"""
        CREATE TABLE IF NOT EXISTS { _fq("INVOICES") } (
            INVOICEID     STRING PRIMARY KEY,
            APPLICATIONID STRING,
            TEAMID        STRING,
            INVOICEDATE   DATE,
            RENEWALDATE   DATE,
            AMOUNT        NUMBER(18,2),
            STATUS        STRING
        )
    """)
    for col_ddl in [
        "FISCAL_YEAR NUMBER(4)",
        "PROGRAMID_AT_BOOKING STRING",
        "VENDORID_AT_BOOKING STRING",
        "GROUPID STRING",
        "GROUPID_AT_BOOKING STRING",
        "PRODUCT_OWNER STRING",
        "AMOUNT_NEXT_YEAR NUMBER(18,2)",
        "CONTRACT_ACTIVE BOOLEAN",
        "COMPANY_CODE STRING",
        "COST_CENTER STRING",
        "SERIAL_NUMBER STRING",
        "WORK_ORDER STRING",
        "AGREEMENT_NUMBER STRING",
        "CONTRACT_DUE NUMBER(4)",
        "SERVICE_TYPE STRING",
        "NOTES STRING",
        "ROLLOVER_BATCH_ID STRING",
        "ROLLED_OVER_FROM_YEAR NUMBER(4)",
    ]:
        execute(f"ALTER TABLE { _fq('INVOICES') } ADD COLUMN IF NOT EXISTS {col_ddl}")

    # INVOICE_TYPE + uniqueness across annual recurring rows
    execute(f"ALTER TABLE { _fq('INVOICES') } ADD COLUMN IF NOT EXISTS INVOICE_TYPE STRING")
    execute(f"UPDATE { _fq('INVOICES') } SET INVOICE_TYPE = 'Recurring Invoice' WHERE INVOICE_TYPE IS NULL")
    try:
        execute(f"ALTER TABLE { _fq('INVOICES') } DROP CONSTRAINT UQ_INVOICE_ANNUAL")
    except Exception:
        pass
    try:
        execute(f"""
            ALTER TABLE { _fq('INVOICES') }
            ADD CONSTRAINT UQ_INVOICE_ANNUAL_TYPE UNIQUE (APPLICATIONID, TEAMID, FISCAL_YEAR, INVOICE_TYPE)
        """)
    except Exception:
        pass


def normalize_team_numeric_types() -> None:
    """
    Normalize team numeric columns to NUMBER(18,2) to match Team FTE behavior.
    """
    for col in ("TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE"):
        try:
            execute(f"ALTER TABLE { _fq('TEAMS') } ALTER COLUMN {col} SET DATA TYPE NUMBER(18,2)")
        except Exception:
            pass

# =========================
# TEAM_CALC: rates & calcs
# =========================

def ensure_team_calc_table() -> None:
    """
    TEAM_CALC stores editable rate columns per team (no duplication of counts).
    Columns created here are NEW per your spec:
      - XOM_RATE
      - CONTRACTOR_CS_RATE
      - CONTRACTOR_C_RATE
    """
    db, sch = _db_and_schema()
    execute(f"""
      CREATE TABLE IF NOT EXISTS {db}.{sch}.TEAM_CALC (
        TEAMID STRING PRIMARY KEY,
        XOM_RATE NUMBER(18,2),
        CONTRACTOR_CS_RATE NUMBER(18,2),
        CONTRACTOR_C_RATE NUMBER(18,2),
        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
      )
    """)

def upsert_team_calc_rates(team_id: str,
                           xom_rate: Optional[float],
                           contractor_cs_rate: Optional[float],
                           contractor_c_rate: Optional[float]) -> None:
    """
    Insert or update TEAM_CALC rates for a team.
    """
    db, sch = _db_and_schema()
    execute(f"""
      MERGE INTO {db}.{sch}.TEAM_CALC t
      USING (
        SELECT %s TEAMID, %s XOM_RATE, %s CONTRACTOR_CS_RATE, %s CONTRACTOR_C_RATE
      ) s
      ON t.TEAMID = s.TEAMID
      WHEN MATCHED THEN UPDATE SET
        XOM_RATE = s.XOM_RATE,
        CONTRACTOR_CS_RATE = s.CONTRACTOR_CS_RATE,
        CONTRACTOR_C_RATE = s.CONTRACTOR_C_RATE,
        UPDATED_AT = CURRENT_TIMESTAMP()
      WHEN NOT MATCHED THEN INSERT (TEAMID, XOM_RATE, CONTRACTOR_CS_RATE, CONTRACTOR_C_RATE)
      VALUES (s.TEAMID, s.XOM_RATE, s.CONTRACTOR_CS_RATE, s.CONTRACTOR_C_RATE)
    """, (team_id, xom_rate, contractor_cs_rate, contractor_c_rate))

def list_team_calc() -> pd.DataFrame:
    """
    Current TEAM_CALC rows with team names.
    """
    return fetch_df(f"""
      SELECT
        tc.TEAMID, t.TEAMNAME,
        tc.XOM_RATE, tc.CONTRACTOR_CS_RATE, tc.CONTRACTOR_C_RATE,
        tc.UPDATED_AT
      FROM { _fq('TEAM_CALC') } tc
      LEFT JOIN { _fq('TEAMS') } t ON t.TEAMID = tc.TEAMID
      ORDER BY t.TEAMNAME
    """)

# =========================================================
# Upserts (core)
# =========================================================

def upsert_program(program_id: str, name: str, owner: Optional[str], fte: Optional[float]) -> None:
    if name and str(name).strip():
        dup = fetch_df(
            f"SELECT PROGRAMID FROM { _fq('PROGRAMS') } WHERE UPPER(PROGRAMNAME)=UPPER(%s) AND PROGRAMID <> %s LIMIT 1",
            (name.strip(), program_id),
        )
        if not dup.empty:
            raise ValueError(f"Program name '{name.strip()}' already exists.")

    sql = f"""
    MERGE INTO { _fq('PROGRAMS') } t
    USING (SELECT %s AS PROGRAMID, %s AS PROGRAMNAME, %s AS PROGRAMOWNER, %s AS PROGRAMFTE) s
    ON t.PROGRAMID = s.PROGRAMID
    WHEN MATCHED THEN UPDATE SET
        PROGRAMNAME = s.PROGRAMNAME,
        PROGRAMOWNER = s.PROGRAMOWNER,
        PROGRAMFTE   = s.PROGRAMFTE
    WHEN NOT MATCHED THEN INSERT (PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE)
    VALUES (s.PROGRAMID, s.PROGRAMNAME, s.PROGRAMOWNER, s.PROGRAMFTE);
    """
    execute(sql, (program_id, name, owner, fte))


def upsert_team(team_id: str, name: str, program_id: Optional[str],
                team_fte: Optional[float],
                delivery_team_fte: Optional[float] = None,
                contractor_c_fte: Optional[float] = None,
                contractor_cs_fte: Optional[float] = None) -> None:
    # Name uniqueness guard (case-insensitive)
    if name and str(name).strip():
        dup = fetch_df(
            f"SELECT TEAMID FROM { _fq('TEAMS') } WHERE UPPER(TEAMNAME)=UPPER(%s) AND TEAMID <> %s LIMIT 1",
            (name.strip(), team_id),
        )
        if not dup.empty:
            raise ValueError(f"Team name '{name.strip()}' already exists.")

    # Ensure columns exist (idempotent)
    execute(f"ALTER TABLE { _fq('TEAMS') } ADD COLUMN IF NOT EXISTS DELIVERY_TEAM_FTE FLOAT")
    execute(f"ALTER TABLE { _fq('TEAMS') } ADD COLUMN IF NOT EXISTS CONTRACTOR_C_FTE FLOAT")
    execute(f"ALTER TABLE { _fq('TEAMS') } ADD COLUMN IF NOT EXISTS CONTRACTOR_CS_FTE FLOAT")

    sql = f"""
    MERGE INTO { _fq('TEAMS') } t
    USING (
      SELECT %s AS TEAMID, %s AS TEAMNAME, %s AS PROGRAMID,
             %s AS TEAMFTE, %s AS DELIVERY_TEAM_FTE, %s AS CONTRACTOR_C_FTE, %s AS CONTRACTOR_CS_FTE
    ) s
    ON t.TEAMID = s.TEAMID
    WHEN MATCHED THEN UPDATE SET
      TEAMNAME = s.TEAMNAME,
      PROGRAMID = s.PROGRAMID,
      TEAMFTE = s.TEAMFTE,
      DELIVERY_TEAM_FTE = s.DELIVERY_TEAM_FTE,
      CONTRACTOR_C_FTE = s.CONTRACTOR_C_FTE,
      CONTRACTOR_CS_FTE = s.CONTRACTOR_CS_FTE
    WHEN NOT MATCHED THEN INSERT
      (TEAMID, TEAMNAME, PROGRAMID, TEAMFTE, DELIVERY_TEAM_FTE, CONTRACTOR_C_FTE, CONTRACTOR_CS_FTE)
    VALUES
      (s.TEAMID, s.TEAMNAME, s.PROGRAMID, s.TEAMFTE, s.DELIVERY_TEAM_FTE, s.CONTRACTOR_C_FTE, s.CONTRACTOR_CS_FTE)
    """
    execute(sql, (team_id, name, program_id, team_fte, delivery_team_fte, contractor_c_fte, contractor_cs_fte))


def upsert_vendor(vendor_id: str, vendor_name: str) -> None:
    if vendor_name and str(vendor_name).strip():
        dup = fetch_df(
            f"SELECT VENDORID FROM { _fq('VENDORS') } WHERE UPPER(VENDORNAME)=UPPER(%s) AND VENDORID <> %s LIMIT 1",
            (vendor_name.strip(), vendor_id),
        )
        if not dup.empty:
            raise ValueError(f"Vendor name '{vendor_name.strip()}' already exists.")

    sql = f"""
    MERGE INTO { _fq('VENDORS') } t
    USING (SELECT %s AS VENDORID, %s AS VENDORNAME) s
    ON t.VENDORID = s.VENDORID
    WHEN MATCHED THEN UPDATE SET VENDORNAME = s.VENDORNAME
    WHEN NOT MATCHED THEN INSERT (VENDORID, VENDORNAME)
    VALUES (s.VENDORID, s.VENDORNAME);
    """
    execute(sql, (vendor_id, vendor_name))


def upsert_application_group(group_id: str, group_name: str, team_id: str,
                             default_vendor_id: Optional[str], owner: Optional[str]) -> None:
    ensure_groups_teamid()
    sql = f"""
    MERGE INTO { _fq('APPLICATION_GROUPS') } t
    USING (
      SELECT %s AS GROUPID, %s AS GROUPNAME, %s AS TEAMID, %s AS DEFAULT_VENDORID, %s AS OWNER
    ) s
    ON t.GROUPID = s.GROUPID
    WHEN MATCHED THEN UPDATE SET
      GROUPNAME = s.GROUPNAME,
      TEAMID = s.TEAMID,
      DEFAULT_VENDORID = s.DEFAULT_VENDORID,
      OWNER = s.OWNER
    WHEN NOT MATCHED THEN INSERT
      (GROUPID, GROUPNAME, TEAMID, DEFAULT_VENDORID, OWNER)
    VALUES
      (s.GROUPID, s.GROUPNAME, s.TEAMID, s.DEFAULT_VENDORID, s.OWNER)
    """
    execute(sql, (group_id, group_name, team_id, default_vendor_id, owner))

    # Keep denormalized PROGRAMID in sync with TEAMS after each change
    execute(f"""
      UPDATE { _fq('APPLICATION_GROUPS') } g
      SET PROGRAMID = t.PROGRAMID
      FROM { _fq('TEAMS') } t
      WHERE g.GROUPID = %s
        AND g.TEAMID = t.TEAMID
        AND (g.PROGRAMID IS NULL OR g.PROGRAMID <> t.PROGRAMID)
    """, (group_id,))


def upsert_application_instance(application_id: str, group_id: str,
                                application_name: str,
                                add_info: Optional[str] = None,
                                vendor_id: Optional[str] = None,
                                **kwargs) -> None:
    """Insert or update an application instance (SITE→ADD_INFO migration compatible)."""
    if add_info is None and "site" in kwargs and kwargs["site"] is not None:
        add_info = kwargs["site"]

    sql = f"""
    MERGE INTO { _fq('APPLICATIONS') } t
    USING (
      SELECT %s AS APPLICATIONID, %s AS GROUPID, %s AS APPLICATIONNAME, %s AS ADD_INFO, %s AS VENDORID
    ) s
    ON t.APPLICATIONID = s.APPLICATIONID
    WHEN MATCHED THEN UPDATE SET
      GROUPID = s.GROUPID,
      APPLICATIONNAME = s.APPLICATIONNAME,
      ADD_INFO = s.ADD_INFO,
      VENDORID = s.VENDORID
    WHEN NOT MATCHED THEN INSERT
      (APPLICATIONID, GROUPID, APPLICATIONNAME, ADD_INFO, VENDORID)
    VALUES
      (s.APPLICATIONID, s.GROUPID, s.APPLICATIONNAME, s.ADD_INFO, s.VENDORID)
    """
    execute(sql, (application_id, group_id, application_name, add_info, vendor_id))


def upsert_invoice(
    invoice_id: str,
    application_id: str,
    team_id: str,
    renewal_date,   # date or ISO string
    amount: float,
    status: str,    # Planned/Completed
    fiscal_year: Optional[int] = None,
    product_owner: Optional[str] = None,
    amount_next_year: Optional[float] = None,
    contract_active: Optional[bool] = True,
    company_code: Optional[str] = None,
    cost_center: Optional[str] = None,
    serial_number: Optional[str] = None,
    work_order: Optional[str] = None,
    agreement_number: Optional[str] = None,
    contract_due: Optional[int] = None,
    service_type: Optional[str] = None,
    notes: Optional[str] = None,
    group_id: Optional[str] = None,
    programid_at_booking: Optional[str] = None,
    vendorid_at_booking: Optional[str] = None,
    groupid_at_booking: Optional[str] = None,
    rollover_batch_id: Optional[str] = None,
    rolled_over_from_year: Optional[int] = None,
    invoice_type: Optional[str] = None,
) -> None:
    eff_type = invoice_type or "Recurring Invoice"
    if eff_type == "Recurring Invoice" and fiscal_year is not None:
        dup_q = f"""
            SELECT COUNT(*) AS CNT
            FROM { _fq('INVOICES') }
            WHERE APPLICATIONID = %s
              AND TEAMID = %s
              AND FISCAL_YEAR = %s
              AND COALESCE(INVOICE_TYPE,'Recurring Invoice') = 'Recurring Invoice'
              AND INVOICEID <> %s
        """
        dup_df = fetch_df(dup_q, (application_id, team_id, int(fiscal_year), invoice_id))
        if not dup_df.empty and int(dup_df.iloc[0]["CNT"]) > 0:
            raise ValueError("A Recurring Invoice already exists for this Application, Team, and Fiscal Year.")

    sql = f"""
    MERGE INTO { _fq('INVOICES') } t
    USING (
        SELECT %s AS INVOICEID, %s AS APPLICATIONID, %s AS TEAMID,
               %s AS RENEWALDATE, %s AS AMOUNT, %s AS STATUS, %s AS FISCAL_YEAR,
               %s AS PRODUCT_OWNER, %s AS AMOUNT_NEXT_YEAR, %s AS CONTRACT_ACTIVE,
               %s AS COMPANY_CODE, %s AS COST_CENTER, %s AS SERIAL_NUMBER, %s AS WORK_ORDER,
               %s AS AGREEMENT_NUMBER, %s AS CONTRACT_DUE, %s AS SERVICE_TYPE, %s AS NOTES,
               %s AS GROUPID, %s AS PROGRAMID_AT_BOOKING, %s AS VENDORID_AT_BOOKING, %s AS GROUPID_AT_BOOKING,
               %s AS ROLLOVER_BATCH_ID, %s AS ROLLED_OVER_FROM_YEAR,
               %s AS INVOICE_TYPE
    ) s
    ON t.INVOICEID = s.INVOICEID
    WHEN MATCHED THEN UPDATE SET
        APPLICATIONID = s.APPLICATIONID,
        TEAMID        = s.TEAMID,
        RENEWALDATE   = s.RENEWALDATE,
        AMOUNT        = s.AMOUNT,
        STATUS        = s.STATUS,
        FISCAL_YEAR   = s.FISCAL_YEAR,
        PRODUCT_OWNER = s.PRODUCT_OWNER,
        AMOUNT_NEXT_YEAR = s.AMOUNT_NEXT_YEAR,
        CONTRACT_ACTIVE  = s.CONTRACT_ACTIVE,
        COMPANY_CODE     = s.COMPANY_CODE,
        COST_CENTER      = s.COST_CENTER,
        SERIAL_NUMBER    = s.SERIAL_NUMBER,
        WORK_ORDER       = s.WORK_ORDER,
        AGREEMENT_NUMBER = s.AGREEMENT_NUMBER,
        CONTRACT_DUE     = s.CONTRACT_DUE,
        SERVICE_TYPE     = s.SERVICE_TYPE,
        NOTES            = s.NOTES,
        GROUPID          = s.GROUPID,
        PROGRAMID_AT_BOOKING = s.PROGRAMID_AT_BOOKING,
        VENDORID_AT_BOOKING  = s.VENDORID_AT_BOOKING,
        GROUPID_AT_BOOKING   = s.GROUPID_AT_BOOKING,
        ROLLOVER_BATCH_ID    = s.ROLLOVER_BATCH_ID,
        ROLLED_OVER_FROM_YEAR = s.ROLLED_OVER_FROM_YEAR,
        INVOICE_TYPE        = COALESCE(s.INVOICE_TYPE, t.INVOICE_TYPE)
    WHEN NOT MATCHED THEN INSERT
        (INVOICEID, APPLICATIONID, TEAMID, RENEWALDATE, AMOUNT, STATUS, FISCAL_YEAR,
         PRODUCT_OWNER, AMOUNT_NEXT_YEAR, CONTRACT_ACTIVE, COMPANY_CODE, COST_CENTER,
         SERIAL_NUMBER, WORK_ORDER, AGREEMENT_NUMBER, CONTRACT_DUE, SERVICE_TYPE, NOTES,
         GROUPID, PROGRAMID_AT_BOOKING, VENDORID_AT_BOOKING, GROUPID_AT_BOOKING,
         ROLLOVER_BATCH_ID, ROLLED_OVER_FROM_YEAR, INVOICE_TYPE)
    VALUES
        (s.INVOICEID, s.APPLICATIONID, s.TEAMID, s.RENEWALDATE, s.AMOUNT, s.STATUS, s.FISCAL_YEAR,
         s.PRODUCT_OWNER, s.AMOUNT_NEXT_YEAR, s.CONTRACT_ACTIVE, s.COMPANY_CODE, s.COST_CENTER,
         s.SERIAL_NUMBER, s.WORK_ORDER, s.AGREEMENT_NUMBER, s.CONTRACT_DUE, s.SERVICE_TYPE, s.NOTES,
         s.GROUPID, s.PROGRAMID_AT_BOOKING, s.VENDORID_AT_BOOKING, s.GROUPID_AT_BOOKING,
         s.ROLLOVER_BATCH_ID, s.ROLLED_OVER_FROM_YEAR, s.INVOICE_TYPE)
    """
    execute(sql, (
        invoice_id, application_id, team_id,
        renewal_date, amount, status, fiscal_year,
        product_owner, amount_next_year, contract_active,
        company_code, cost_center, serial_number, work_order,
        agreement_number, contract_due, service_type, notes,
        group_id, programid_at_booking, vendorid_at_booking, groupid_at_booking,
        rollover_batch_id, rolled_over_from_year,
        eff_type,
    ))

# Backward compatibility wrapper
def upsert_invoice_extended(
    invoice_id: str,
    application_id: str,
    team_id: str,
    renewal_date,
    amount: float,
    status: str,
    fiscal_year: Optional[int] = None,
    product_owner: Optional[str] = None,
    amount_next_year: Optional[float] = None,
    contract_active: Optional[bool] = True,
    company_code: Optional[str] = None,
    cost_center: Optional[str] = None,
    serial_number: Optional[str] = None,
    work_order: Optional[str] = None,
    agreement_number: Optional[str] = None,
    contract_due: Optional[int] = None,
    service_type: Optional[str] = None,
    notes: Optional[str] = None,
    group_id: Optional[str] = None,
    programid_at_booking: Optional[str] = None,
    vendorid_at_booking: Optional[str] = None,
    groupid_at_booking: Optional[str] = None,
    rollover_batch_id: Optional[str] = None,
    rolled_over_from_year: Optional[int] = None,
    invoice_type: Optional[str] = None,
) -> None:
    return upsert_invoice(
        invoice_id=invoice_id,
        application_id=application_id,
        team_id=team_id,
        renewal_date=renewal_date,
        amount=amount,
        status=status,
        fiscal_year=fiscal_year,
        product_owner=product_owner,
        amount_next_year=amount_next_year,
        contract_active=contract_active,
        company_code=company_code,
        cost_center=cost_center,
        serial_number=serial_number,
        work_order=work_order,
        agreement_number=agreement_number,
        contract_due=contract_due,
        service_type=service_type,
        notes=notes,
        group_id=group_id,
        programid_at_booking=programid_at_booking,
        vendorid_at_booking=vendorid_at_booking,
        groupid_at_booking=groupid_at_booking,
        rollover_batch_id=rollover_batch_id,
        rolled_over_from_year=rolled_over_from_year,
        invoice_type=invoice_type,
    )

# =========================================================
# Deletes (with safety checks)
# =========================================================

def delete_program(program_id: str) -> None:
    execute(f"DELETE FROM { _fq('PROGRAMS') } WHERE PROGRAMID = %s", (program_id,))

def delete_team(team_id: str) -> None:
    execute(f"DELETE FROM { _fq('TEAMS') } WHERE TEAMID = %s", (team_id,))

def delete_vendor(vendor_id: str) -> None:
    execute(f"DELETE FROM { _fq('VENDORS') } WHERE VENDORID = %s", (vendor_id,))

def delete_application_group(group_id: str) -> None:
    df = fetch_df(f"SELECT COUNT(*) AS N FROM { _fq('APPLICATIONS') } WHERE GROUPID = %s", (group_id,))
    n = int(df.iloc[0]["N"]) if not df.empty else 0
    if n > 0:
        raise ValueError("Cannot delete this Application Group because it has Application Instances. Delete the instances first.")
    execute(f"DELETE FROM { _fq('APPLICATION_GROUPS') } WHERE GROUPID = %s", (group_id,))

def delete_application(application_id: str) -> None:
    df = fetch_df(f"SELECT COUNT(*) AS N FROM { _fq('INVOICES') } WHERE APPLICATIONID = %s", (application_id,))
    n = int(df.iloc[0]["N"]) if not df.empty else 0
    if n > 0:
        raise ValueError("Cannot delete this Application Instance because it has linked Invoices.")
    execute(f"DELETE FROM { _fq('APPLICATIONS') } WHERE APPLICATIONID = %s", (application_id,))

def delete_invoice(invoice_id: str) -> None:
    execute(f"DELETE FROM { _fq('INVOICES') } WHERE INVOICEID = %s", (invoice_id,))


# =========================================================
# List/lookup helpers (used by pages)
# =========================================================

def list_programs() -> pd.DataFrame:
    return fetch_df(f"""
        SELECT PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE
        FROM { _fq('PROGRAMS') }
        ORDER BY PROGRAMNAME
    """)


def list_teams() -> pd.DataFrame:
    return fetch_df(f"""
        SELECT
            TEAMID,
            TEAMNAME,
            PROGRAMID,
            PRODUCTOWNER,
            TO_DECIMAL(TEAMFTE,           18, 2) AS TEAMFTE,
            TO_DECIMAL(DELIVERY_TEAM_FTE, 18, 2) AS DELIVERY_TEAM_FTE,
            TO_DECIMAL(CONTRACTOR_C_FTE,  18, 2) AS CONTRACTOR_C_FTE,
            TO_DECIMAL(CONTRACTOR_CS_FTE, 18, 2) AS CONTRACTOR_CS_FTE
        FROM { _fq('TEAMS') }
        ORDER BY TEAMNAME
    """)


def list_vendors() -> pd.DataFrame:
    return fetch_df(f"""
        SELECT VENDORID, VENDORNAME
        FROM { _fq('VENDORS') }
        ORDER BY VENDORNAME
    """)


def list_application_groups(team_id: Optional[str] = None) -> pd.DataFrame:
    ensure_groups_teamid()
    where = "WHERE g.TEAMID = %s" if team_id else ""
    params: Optional[tuple] = (team_id,) if team_id else None
    return fetch_df(f"""
        SELECT
            g.GROUPID,
            g.GROUPNAME,
            g.TEAMID,
            t.TEAMNAME,
            COALESCE(g.PROGRAMID, t.PROGRAMID) AS PROGRAMID,
            p.PROGRAMNAME,
            g.DEFAULT_VENDORID AS VENDORID,
            v.VENDORNAME,
            g.OWNER,
            g.CREATED_AT
        FROM { _fq('APPLICATION_GROUPS') } g
        LEFT JOIN { _fq('TEAMS') }    t ON t.TEAMID    = g.TEAMID
        LEFT JOIN { _fq('PROGRAMS') } p ON p.PROGRAMID = COALESCE(g.PROGRAMID, t.PROGRAMID)
        LEFT JOIN { _fq('VENDORS') }  v ON v.VENDORID  = g.DEFAULT_VENDORID
        {where}
        ORDER BY g.GROUPNAME
    """, params)


def list_groups_for_team(team_id: str) -> pd.DataFrame:
    ensure_groups_teamid()
    return fetch_df(f"""
        SELECT
            g.GROUPID,
            g.GROUPNAME,
            g.TEAMID,
            t.TEAMNAME,
            COALESCE(g.PROGRAMID, t.PROGRAMID) AS PROGRAMID,
            p.PROGRAMNAME,
            g.DEFAULT_VENDORID AS VENDORID,
            v.VENDORNAME,
            g.OWNER,
            g.CREATED_AT
        FROM { _fq('APPLICATION_GROUPS') } g
        LEFT JOIN { _fq('TEAMS') }    t ON t.TEAMID    = g.TEAMID
        LEFT JOIN { _fq('PROGRAMS') } p ON p.PROGRAMID = COALESCE(g.PROGRAMID, t.PROGRAMID)
        LEFT JOIN { _fq('VENDORS') }  v ON v.VENDORID = g.DEFAULT_VENDORID
        WHERE g.TEAMID = %s
        ORDER BY g.GROUPNAME
    """, (team_id,))

def list_group_team_links(team_id: Optional[str] = None) -> pd.DataFrame:
    ensure_groups_teamid()
    where = "WHERE g.TEAMID = %s" if team_id else ""
    params = (team_id,) if team_id else None
    return fetch_df(f"""
        SELECT
          g.GROUPID,
          g.GROUPNAME,
          g.TEAMID,
          t.TEAMNAME
        FROM { _fq('APPLICATION_GROUPS') } g
        LEFT JOIN { _fq('TEAMS') } t ON t.TEAMID = g.TEAMID
        {where}
        ORDER BY g.GROUPNAME
    """, params)


def list_applications(team_id: Optional[str] = None, group_id: Optional[str] = None) -> pd.DataFrame:
    """
    List application instances. Optionally filter by team (via group’s team) and/or group.
    """
    ensure_groups_teamid()
    where_clauses = []
    params: List[Any] = []
    if team_id:
        where_clauses.append("g.TEAMID = %s")
        params.append(team_id)
    if group_id:
        where_clauses.append("a.GROUPID = %s")
        params.append(group_id)
    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    return fetch_df(f"""
        SELECT
            a.APPLICATIONID,
            a.APPLICATIONNAME,
            a.ADD_INFO AS ADD_INFO,
            a.VENDORID,
            v.VENDORNAME,
            a.GROUPID,
            g.GROUPNAME,
            g.TEAMID,
            t.TEAMNAME
        FROM { _fq('APPLICATIONS') } a
        LEFT JOIN { _fq('APPLICATION_GROUPS') } g ON g.GROUPID = a.GROUPID
        LEFT JOIN { _fq('TEAMS') } t ON t.TEAMID = g.TEAMID
        LEFT JOIN { _fq('VENDORS') } v ON v.VENDORID = a.VENDORID
        {where_sql}
        ORDER BY g.GROUPNAME, a.APPLICATIONNAME, a.ADD_INFO
    """, tuple(params) if params else None)


def list_invoices(
    fiscal_year: Optional[int] = None,
    team_id: Optional[str] = None,
    group_id: Optional[str] = None,
    application_id: Optional[str] = None,
    quarter: Optional[int] = None,
    status: Optional[str] = None,
    vendor_id: Optional[str] = None,
) -> pd.DataFrame:
    clauses: List[str] = []
    params: List[Any] = []

    if fiscal_year is not None:
        clauses.append("i.FISCAL_YEAR = %s")
        params.append(int(fiscal_year))
    if team_id:
        clauses.append("i.TEAMID = %s")
        params.append(team_id)
    if group_id:
        clauses.append("i.GROUPID = %s")
        params.append(group_id)
    if application_id:
        clauses.append("i.APPLICATIONID = %s")
        params.append(application_id)
    if quarter:
        clauses.append("i.QUARTER = %s")
        params.append(int(quarter))
    if status:
        clauses.append("i.STATUS = %s")
        params.append(status)
    if vendor_id:
        clauses.append("a.VENDORID = %s")
        params.append(vendor_id)

    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    return fetch_df(f"""
        SELECT
            i.INVOICEID,
            i.APPLICATIONID,
            i.TEAMID,
            i.GROUPID,
            i.FISCAL_YEAR,
            i.RENEWALDATE,
            i.AMOUNT,
            i.STATUS,
            i.AMOUNT_NEXT_YEAR,
            i.CONTRACT_ACTIVE,
            i.COMPANY_CODE,
            i.COST_CENTER,
            i.SERIAL_NUMBER,
            i.WORK_ORDER,
            i.AGREEMENT_NUMBER,
            i.CONTRACT_DUE,
            i.SERVICE_TYPE,
            i.NOTES,
            i.PROGRAMID_AT_BOOKING,
            i.VENDORID_AT_BOOKING,
            i.GROUPID_AT_BOOKING,
            COALESCE(i.INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE
        FROM { _fq('INVOICES') } i
        LEFT JOIN { _fq('APPLICATIONS') } a ON a.APPLICATIONID = i.APPLICATIONID
        {where_sql}
        ORDER BY COALESCE(i.RENEWALDATE, TO_DATE('1900-01-01')) DESC, i.AMOUNT DESC, i.INVOICEID
    """, tuple(params) if params else None)


# =========================================================
# Repairs / utilities
# =========================================================

def repair_programs_programfte() -> None:
    """Ensure PROGRAMFTE is numeric (lenient coercion)."""
    df = fetch_df(f"SELECT PROGRAMID, PROGRAMFTE FROM { _fq('PROGRAMS') }")
    if df.empty:
        return
    updates: List[Tuple[float, str]] = []
    for _, row in df.iterrows():
        pid = row.get("PROGRAMID")
        val = row.get("PROGRAMFTE")
        if pd.isna(val):
            continue
        try:
            num = float(val)
        except Exception:
            s = str(val).strip().replace("%", "").replace(",", ".")
            try:
                num = float(s)
            except Exception:
                continue
        updates.append((num, pid))
    if updates:
        execute(
            f"UPDATE { _fq('PROGRAMS') } SET PROGRAMFTE = %s WHERE PROGRAMID = %s",
            updates,
            many=True,
        )

def repair_team_fte_values() -> None:
    """
    Coerce TEAMS FTE columns that might be stored as VARCHAR (e.g., '5,0') into numeric.
    - Replaces comma decimals with dots
    - Trims blanks to NULL
    - Uses TRY_TO_DECIMAL to avoid errors
    - Then enforces NUMBER(18,2) on the columns
    """
    db, sch = _db_and_schema()
    for col in ("TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE"):
        try:
            execute(f"""
                UPDATE {db}.{sch}.TEAMS
                SET {col} = CAST(
                    TRY_TO_DECIMAL(
                        REPLACE(NULLIF(TRIM(TO_VARCHAR({col})), ''), ',', '.'),
                        18, 2
                    ) AS NUMBER(18,2)
                )
            """)
        except Exception:
            pass

    # Enforce numeric data types after cleaning
    for col in ("TEAMFTE", "DELIVERY_TEAM_FTE", "CONTRACTOR_C_FTE", "CONTRACTOR_CS_FTE"):
        try:
            execute(f"ALTER TABLE {db}.{sch}.TEAMS ALTER COLUMN {col} SET DATA TYPE NUMBER(18,2)")
        except Exception:
            pass


# =========================================================
# ADO minimal schema + cleanup
# =========================================================

def ensure_ado_minimal_tables() -> None:
    db, sch = _db_and_schema()

    execute(f"""
      CREATE TABLE IF NOT EXISTS {db}.{sch}.ADO_FEATURES (
        FEATURE_ID     STRING PRIMARY KEY,
        TITLE          STRING,
        STATE          STRING,
        TEAM_RAW       STRING,
        APP_NAME_RAW   STRING,
        EFFORT_POINTS  FLOAT,
        ITERATION_PATH STRING,
        CREATED_AT     TIMESTAMP_NTZ,
        CHANGED_AT     TIMESTAMP_NTZ,
        ADO_YEAR       NUMBER(4)           -- <-- NEW (nullable, from Excel "Year")
      )
    """)

    # If table already exists but column doesn't, add it now:
    try:
        execute(f"ALTER TABLE {db}.{sch}.ADO_FEATURES ADD COLUMN IF NOT EXISTS ADO_YEAR NUMBER(4)")
    except Exception:
        pass

    # Mapping tables (unchanged)...
    execute(f"""
      CREATE TABLE IF NOT EXISTS {db}.{sch}.MAP_ADO_TEAM_TO_TCO_TEAM (
        ADO_TEAM STRING PRIMARY KEY,
        TEAMID   STRING
      )
    """)
    execute(f"""
      CREATE TABLE IF NOT EXISTS {db}.{sch}.MAP_ADO_APP_TO_TCO_GROUP (
        ADO_APP   STRING PRIMARY KEY,
        APP_GROUP STRING
      )
    """)



def reset_ado_calc_artifacts(drop_mappings: bool = False) -> None:
    """
    Remove calculation artifacts (tables/columns) created earlier.
    """
    db, sch = _db_and_schema()

    for tbl in ["ADO_FEATURE_COST_ESTIMATE", "EFFORT_SPLIT_RULES"]:
        try:
            execute(f"DROP TABLE IF EXISTS {db}.{sch}.{tbl}")
        except Exception:
            pass

    # Try to drop any known calc columns on ADO_FEATURES
    for col in [
        "ADO_YEAR", "POINTS_PER_FTE_PER_PI",
        "EFF_TEAM","EFF_DELIVERY","EFF_CS","EFF_C",
        "FTEPI_TEAM","FTEPI_DELIVERY","FTEPI_CS","FTEPI_C",
        "COST_TEAM","COST_DELIVERY","COST_CS","COST_C",
        "EST_FTE_PI","EST_COST_PI"
    ]:
        try:
            if _table_has_column(db, sch, "ADO_FEATURES", col):
                execute(f"ALTER TABLE {db}.{sch}.ADO_FEATURES DROP COLUMN {col}")
        except Exception:
            pass

    if drop_mappings:
        for tbl in ["MAP_ADO_TEAM_TO_TCO_TEAM", "MAP_ADO_APP_TO_TCO_GROUP"]:
            try:
                execute(f"DROP TABLE IF EXISTS {db}.{sch}.{tbl}")
            except Exception:
                pass

    ensure_ado_minimal_tables()


# ---------- NEW: EFFORT_POINTS precision repair & upserts ----------

def repair_ado_effort_points_precision() -> None:
    """
    1) Ensure ADO_FEATURES.EFFORT_POINTS is FLOAT (keeps decimals).
    2) Clean existing rows where points may be strings or use comma decimals.
       Anything unparsable becomes NULL.
    """
    db, sch = _db_and_schema()
    # Ensure type is FLOAT
    try:
        execute(f"ALTER TABLE {db}.{sch}.ADO_FEATURES ALTER COLUMN EFFORT_POINTS SET DATA TYPE FLOAT")
    except Exception:
        pass

    # Normalize values: to_varchar -> replace comma -> try_to_decimal -> cast float
    try:
        execute(f"""
            UPDATE {db}.{sch}.ADO_FEATURES
            SET EFFORT_POINTS = CAST(
                TRY_TO_DECIMAL(
                    REPLACE(NULLIF(TRIM(TO_VARCHAR(EFFORT_POINTS)), ''), ',', '.'),
                    18, 6
                ) AS FLOAT
            )
            WHERE TRUE
        """)
    except Exception:
        pass


def upsert_ado_feature(
    feature_id: str,
    title: Optional[str] = None,
    state: Optional[str] = None,
    team_raw: Optional[str] = None,
    app_name_raw: Optional[str] = None,
    effort_points: Optional[float] = None,
    iteration_path: Optional[str] = None,
    created_at: Optional[str] = None,
    changed_at: Optional[str] = None,
) -> None:
    """Insert/update a single ADO feature with decimal-preserving effort points."""
    db, sch = _db_and_schema()
    ensure_ado_minimal_tables()

    sql = f"""
      MERGE INTO {db}.{sch}.ADO_FEATURES t
      USING (
        SELECT %s FEATURE_ID, %s TITLE, %s STATE, %s TEAM_RAW, %s APP_NAME_RAW,
               %s EFFORT_POINTS, %s ITERATION_PATH, %s CREATED_AT, %s CHANGED_AT
      ) s
      ON t.FEATURE_ID = s.FEATURE_ID
      WHEN MATCHED THEN UPDATE SET
        TITLE          = s.TITLE,
        STATE          = s.STATE,
        TEAM_RAW       = s.TEAM_RAW,
        APP_NAME_RAW   = s.APP_NAME_RAW,
        EFFORT_POINTS  = s.EFFORT_POINTS,
        ITERATION_PATH = s.ITERATION_PATH,
        CREATED_AT     = s.CREATED_AT,
        CHANGED_AT     = s.CHANGED_AT
      WHEN NOT MATCHED THEN INSERT
        (FEATURE_ID, TITLE, STATE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, ITERATION_PATH, CREATED_AT, CHANGED_AT)
      VALUES
        (s.FEATURE_ID, s.TITLE, s.STATE, s.TEAM_RAW, s.APP_NAME_RAW, s.EFFORT_POINTS, s.ITERATION_PATH, s.CREATED_AT, s.CHANGED_AT)
    """
    execute(sql, (
        feature_id, title, state, team_raw, app_name_raw,
        None if effort_points is None else float(str(effort_points).replace(",", ".")),
        iteration_path, created_at, changed_at
    ))


def bulk_upsert_ado_features(rows: Iterable[Dict[str, Any]]) -> None:
    """Batch upsert for ADO features; keeps decimal effort points intact."""
    db, sch = _db_and_schema()
    ensure_ado_minimal_tables()

    params: List[Tuple[Any, ...]] = []
    for r in rows:
        ep = r.get("effort_points")
        ep = None if ep in (None, "") else float(str(ep).replace(",", "."))
        params.append((
            r.get("feature_id"),
            r.get("title"),
            r.get("state"),
            r.get("team_raw"),
            r.get("app_name_raw"),
            ep,
            r.get("iteration_path"),
            r.get("created_at"),
            r.get("changed_at"),
        ))

    sql = f"""
      MERGE INTO {db}.{sch}.ADO_FEATURES t
      USING (
        SELECT
          %s AS FEATURE_ID, %s AS TITLE, %s AS STATE, %s AS TEAM_RAW, %s AS APP_NAME_RAW,
          %s AS EFFORT_POINTS, %s AS ITERATION_PATH, %s AS CREATED_AT, %s AS CHANGED_AT
      ) s
      ON t.FEATURE_ID = s.FEATURE_ID
      WHEN MATCHED THEN UPDATE SET
        TITLE          = s.TITLE,
        STATE          = s.STATE,
        TEAM_RAW       = s.TEAM_RAW,
        APP_NAME_RAW   = s.APP_NAME_RAW,
        EFFORT_POINTS  = s.EFFORT_POINTS,
        ITERATION_PATH = s.ITERATION_PATH,
        CREATED_AT     = s.CREATED_AT,
        CHANGED_AT     = s.CHANGED_AT
      WHEN NOT MATCHED THEN INSERT
        (FEATURE_ID, TITLE, STATE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, ITERATION_PATH, CREATED_AT, CHANGED_AT)
      VALUES
        (s.FEATURE_ID, s.TITLE, s.STATE, s.TEAM_RAW, s.APP_NAME_RAW, s.EFFORT_POINTS, s.ITERATION_PATH, s.CREATED_AT, s.CHANGED_AT)
    """
    execute(sql, params, many=True)


# ---------- NEW: Mapping helpers ----------

def upsert_map_ado_team_to_tco_team(ado_team: str, team_id: Optional[str]) -> None:
    """Map an ADO team name to a TCO TEAMID (nullable unmaps)."""
    db, sch = _db_and_schema()
    ensure_ado_minimal_tables()
    sql = f"""
      MERGE INTO {db}.{sch}.MAP_ADO_TEAM_TO_TCO_TEAM t
      USING (SELECT %s ADO_TEAM, %s TEAMID) s
      ON t.ADO_TEAM = s.ADO_TEAM
      WHEN MATCHED THEN UPDATE SET TEAMID = s.TEAMID
      WHEN NOT MATCHED THEN INSERT (ADO_TEAM, TEAMID) VALUES (s.ADO_TEAM, s.TEAMID)
    """
    execute(sql, (ado_team, team_id))

def upsert_map_ado_app_to_tco_group(ado_app: str, app_group: Optional[str]) -> None:
    """Map an ADO application name to a TCO GROUPID (nullable unmaps)."""
    db, sch = _db_and_schema()
    ensure_ado_minimal_tables()
    sql = f"""
      MERGE INTO {db}.{sch}.MAP_ADO_APP_TO_TCO_GROUP t
      USING (SELECT %s ADO_APP, %s APP_GROUP) s
      ON t.ADO_APP = s.ADO_APP
      WHEN MATCHED THEN UPDATE SET APP_GROUP = s.APP_GROUP
      WHEN NOT MATCHED THEN INSERT (ADO_APP, APP_GROUP) VALUES (s.ADO_APP, s.APP_GROUP)
    """
    execute(sql, (ado_app, app_group))

def list_map_ado_team() -> pd.DataFrame:
    db, sch = _db_and_schema()
    ensure_ado_minimal_tables()
    return fetch_df(f"""
      SELECT m.ADO_TEAM, m.TEAMID, t.TEAMNAME
      FROM {db}.{sch}.MAP_ADO_TEAM_TO_TCO_TEAM m
      LEFT JOIN {db}.{sch}.TEAMS t ON t.TEAMID = m.TEAMID
      ORDER BY m.ADO_TEAM
    """)

def list_map_ado_app() -> pd.DataFrame:
    db, sch = _db_and_schema()
    ensure_ado_minimal_tables()
    return fetch_df(f"""
      SELECT m.ADO_APP, m.APP_GROUP, g.GROUPNAME
      FROM {db}.{sch}.MAP_ADO_APP_TO_TCO_GROUP m
      LEFT JOIN {db}.{sch}.APPLICATION_GROUPS g ON g.GROUPID = m.APP_GROUP
      ORDER BY m.ADO_APP
    """)


# =========================================================
# Views: list & drop helpers
# =========================================================

def ensure_team_cost_view() -> None:
    """
    Creates a per-feature view that:
      - exposes ADO_YEAR and ITERATION_NUM (parsed from ITERATION_PATH)
      - uses team FTE composition + rates
      - allocates the fixed team PI cost across features so that summing over features
        for a Team/Year/Iteration equals the team’s PI cost exactly.
    """
    db, sch = _db_and_schema()
    execute(f"""
      CREATE OR REPLACE VIEW {db}.{sch}.VW_TEAM_COSTS_PER_FEATURE AS
      WITH f AS (
        SELECT
          af.FEATURE_ID,
          af.TITLE,
          af.STATE,
          af.TEAM_RAW,
          af.APP_NAME_RAW,
          af.EFFORT_POINTS,
          af.ITERATION_PATH,
          af.CREATED_AT,
          af.CHANGED_AT,
          /* Take ADO_YEAR from ADO_FEATURES if present; fall back to year of CHANGED/CREATED */
          COALESCE(af.ADO_YEAR, YEAR(COALESCE(af.CHANGED_AT, af.CREATED_AT))) AS ADO_YEAR,
          /* Iteration number like I1, I 2, Iteration 3 → 1/2/3 */
          TRY_TO_NUMBER(REGEXP_SUBSTR(af.ITERATION_PATH,
                     'I[[:space:]]*([0-9]+)', 1, 1, 'i', 1)) AS ITERATION_NUM
        FROM {db}.{sch}.ADO_FEATURES af
      ),
      j AS (
        SELECT
          f.*,
          m.TEAMID,
          t.TEAMNAME,
          /* Team FTEs (coerced to FLOAT) */
          COALESCE(CAST(t.DELIVERY_TEAM_FTE  AS FLOAT), 0.0) AS DELIVERY_TEAM_FTE,
          COALESCE(CAST(t.CONTRACTOR_CS_FTE  AS FLOAT), 0.0) AS CONTRACTOR_CS_FTE,
          COALESCE(CAST(t.CONTRACTOR_C_FTE   AS FLOAT), 0.0) AS CONTRACTOR_C_FTE,
          COALESCE(CAST(t.TEAMFTE            AS FLOAT), 0.0) AS TEAMFTE,
          /* Rates (FLOAT) */
          COALESCE(CAST(tc.XOM_RATE           AS FLOAT), 0.0) AS XOM_RATE,
          COALESCE(CAST(tc.CONTRACTOR_CS_RATE AS FLOAT), 0.0) AS CONTRACTOR_CS_RATE,
          COALESCE(CAST(tc.CONTRACTOR_C_RATE  AS FLOAT), 0.0) AS CONTRACTOR_C_RATE
        FROM f
        LEFT JOIN {db}.{sch}.MAP_ADO_TEAM_TO_TCO_TEAM m
          ON m.ADO_TEAM = f.TEAM_RAW
        LEFT JOIN {db}.{sch}.TEAMS t
          ON t.TEAMID = m.TEAMID
        LEFT JOIN {db}.{sch}.TEAM_CALC tc
          ON tc.TEAMID = t.TEAMID
      ),
      /* Team-year-iteration aggregates to allocate fixed team PI cost */
      team_pi AS (
        SELECT
          TEAMID,
          ADO_YEAR,
          ITERATION_NUM,
          SUM(COALESCE(EFFORT_POINTS, 0))            AS TOTAL_EFFORT_POINTS,
          COUNT(*)                                    AS FEATURE_COUNT,
          /* This is the fixed per-PI team cost for the period */
          MAX(COALESCE(TEAMFTE,0) * COALESCE(XOM_RATE,0) / 4.0) AS TEAM_PI_FIXED_COST
        FROM j
        GROUP BY TEAMID, ADO_YEAR, ITERATION_NUM
      )
      SELECT
        j.FEATURE_ID,
        j.TITLE,
        j.STATE,
        j.TEAM_RAW,
        j.APP_NAME_RAW,
        j.EFFORT_POINTS,
        j.ITERATION_PATH,
        j.CREATED_AT,
        j.CHANGED_AT,
        j.ADO_YEAR,
        j.ITERATION_NUM,
        /* Composite key like 2025-I1 (nullable-friendly) */
        CASE
          WHEN j.ADO_YEAR IS NOT NULL AND j.ITERATION_NUM IS NOT NULL
          THEN j.ADO_YEAR || '-I' || j.ITERATION_NUM
          ELSE NULL
        END AS ADO_PI_KEY,

        /* Team & Rates context */
        j.TEAMID,
        j.TEAMNAME,
        j.DELIVERY_TEAM_FTE,
        j.CONTRACTOR_CS_FTE,
        j.CONTRACTOR_C_FTE,
        j.TEAMFTE,
        j.XOM_RATE,
        j.CONTRACTOR_CS_RATE,
        j.CONTRACTOR_C_RATE,

        /* Denominator for effort‑based split */
        (j.DELIVERY_TEAM_FTE + j.CONTRACTOR_CS_FTE + j.CONTRACTOR_C_FTE) AS COMP_DENOM,

        /* Allocation helpers from team_pi */
        tp.TOTAL_EFFORT_POINTS,
        tp.FEATURE_COUNT,
        tp.TEAM_PI_FIXED_COST,

        /* === COSTS PER FEATURE (PI) ====================================== */

        /* (A) Fixed Team PI cost ALLOCATED to features.
           Prefer effort‑proportional allocation. If total effort is 0, split evenly. */
        CAST(
          CASE
            WHEN tp.TOTAL_EFFORT_POINTS > 0
              THEN tp.TEAM_PI_FIXED_COST * (COALESCE(j.EFFORT_POINTS,0) / tp.TOTAL_EFFORT_POINTS)
            WHEN tp.FEATURE_COUNT > 0
              THEN tp.TEAM_PI_FIXED_COST / tp.FEATURE_COUNT
            ELSE 0
          END
        AS FLOAT) AS TEAM_COST_PERPI,

        /* (B) Delivery share * effort * XOM rate */
        CAST(
          CASE
            WHEN (j.DELIVERY_TEAM_FTE + j.CONTRACTOR_CS_FTE + j.CONTRACTOR_C_FTE) = 0
              THEN 0
            ELSE (j.DELIVERY_TEAM_FTE / (j.DELIVERY_TEAM_FTE + j.CONTRACTOR_CS_FTE + j.CONTRACTOR_C_FTE))
          END
          * COALESCE(j.EFFORT_POINTS,0) * j.XOM_RATE
        AS FLOAT) AS DEL_TEAM_COST_PERPI,

        /* (C) Contractor CS share * effort * CS rate */
        CAST(
          CASE
            WHEN (j.DELIVERY_TEAM_FTE + j.CONTRACTOR_CS_FTE + j.CONTRACTOR_C_FTE) = 0
              THEN 0
            ELSE (j.CONTRACTOR_CS_FTE / (j.DELIVERY_TEAM_FTE + j.CONTRACTOR_CS_FTE + j.CONTRACTOR_C_FTE))
          END
          * COALESCE(j.EFFORT_POINTS,0) * j.CONTRACTOR_CS_RATE
        AS FLOAT) AS TEAM_CONTRACTOR_CS_COST_PERPI,

        /* (D) Contractor C share * effort * C rate */
        CAST(
          CASE
            WHEN (j.DELIVERY_TEAM_FTE + j.CONTRACTOR_CS_FTE + j.CONTRACTOR_C_FTE) = 0
              THEN 0
            ELSE (j.CONTRACTOR_C_FTE / (j.DELIVERY_TEAM_FTE + j.CONTRACTOR_CS_FTE + j.CONTRACTOR_C_FTE))
          END
          * COALESCE(j.EFFORT_POINTS,0) * j.CONTRACTOR_C_RATE
        AS FLOAT) AS TEAM_CONTRACTOR_C_COST_PERPI

      FROM j
      LEFT JOIN team_pi tp
        ON tp.TEAMID        = j.TEAMID
       AND tp.ADO_YEAR      = j.ADO_YEAR
       AND tp.ITERATION_NUM = j.ITERATION_NUM
    """)

    db, sch = _db_and_schema()
    execute(f"""
      CREATE OR REPLACE VIEW {db}.{sch}.VW_TEAM_COSTS_PER_FEATURE AS
      WITH j AS (
        SELECT
          af.FEATURE_ID,
          af.TITLE,
          af.STATE,
          af.TEAM_RAW,
          af.APP_NAME_RAW,
          CAST(af.EFFORT_POINTS AS FLOAT) AS EFFORT_POINTS,
          af.ITERATION_PATH,
          af.CREATED_AT,
          af.CHANGED_AT,
          af.ADO_YEAR                                               AS ADO_YEAR_EXCEL,   -- <-- from Excel

          m.TEAMID,
          t.TEAMNAME,

          -- FTEs (robust cast to FLOAT)
          CAST(COALESCE(TRY_TO_DECIMAL(REPLACE(NULLIF(TRIM(TO_VARCHAR(t.DELIVERY_TEAM_FTE)), ''), ',', '.'), 18, 6), 0) AS FLOAT) AS DELIVERY_TEAM_FTE,
          CAST(COALESCE(TRY_TO_DECIMAL(REPLACE(NULLIF(TRIM(TO_VARCHAR(t.CONTRACTOR_CS_FTE)), ''), ',', '.'), 18, 6), 0) AS FLOAT) AS CONTRACTOR_CS_FTE,
          CAST(COALESCE(TRY_TO_DECIMAL(REPLACE(NULLIF(TRIM(TO_VARCHAR(t.CONTRACTOR_C_FTE)), ''), ',', '.'), 18, 6), 0) AS FLOAT) AS CONTRACTOR_C_FTE,
          CAST(COALESCE(TRY_TO_DECIMAL(REPLACE(NULLIF(TRIM(TO_VARCHAR(t.TEAMFTE)), ''), ',', '.'), 18, 6), 0) AS FLOAT) AS TEAMFTE,

          -- Rates
          CAST(COALESCE(tc.XOM_RATE, 0) AS FLOAT)           AS XOM_RATE,
          CAST(COALESCE(tc.CONTRACTOR_CS_RATE, 0) AS FLOAT) AS CONTRACTOR_CS_RATE,
          CAST(COALESCE(tc.CONTRACTOR_C_RATE, 0) AS FLOAT)  AS CONTRACTOR_C_RATE,

          -- Regex helpers (Snowflake: pass 'i' for case-insensitive)
          REGEXP_SUBSTR(af.ITERATION_PATH, '(19|20)[0-9]{2}')                                           AS YEAR4_STR,
          REGEXP_REPLACE(af.ITERATION_PATH, '.*FY\\s*([0-9]{2,4}).*', '\\1', 1, 1, 'i')                  AS FY_DIGITS_STR,
          REGEXP_REPLACE(af.ITERATION_PATH, '.*(PI|ITERATION|I)\\s*-?\\s*([0-9]+).*', '\\2', 1, 1, 'i')  AS ITER_NUM_STR
        FROM {db}.{sch}.ADO_FEATURES af
        LEFT JOIN {db}.{sch}.MAP_ADO_TEAM_TO_TCO_TEAM m
          ON m.ADO_TEAM = af.TEAM_RAW
        LEFT JOIN {db}.{sch}.TEAMS t
          ON t.TEAMID = m.TEAMID
        LEFT JOIN {db}.{sch}.TEAM_CALC tc
          ON tc.TEAMID = t.TEAMID
      )
      , parsed AS (
        SELECT
          FEATURE_ID, TITLE, STATE, TEAM_RAW, APP_NAME_RAW, EFFORT_POINTS, ITERATION_PATH, CREATED_AT, CHANGED_AT,
          TEAMID, TEAMNAME,
          DELIVERY_TEAM_FTE, CONTRACTOR_CS_FTE, CONTRACTOR_C_FTE, TEAMFTE,
          XOM_RATE, CONTRACTOR_CS_RATE, CONTRACTOR_C_RATE,

          -- Year precedence: Excel ADO_YEAR -> explicit YYYY -> FYxx/FYyyyy -> timestamp year
          COALESCE(
            ADO_YEAR_EXCEL,
            TRY_TO_NUMBER(YEAR4_STR),
            CASE
              WHEN TRY_TO_NUMBER(FY_DIGITS_STR) IS NOT NULL THEN
                CASE WHEN LENGTH(FY_DIGITS_STR) = 2
                     THEN 2000 + TRY_TO_NUMBER(FY_DIGITS_STR)  -- FY25 -> 2025
                     ELSE TRY_TO_NUMBER(FY_DIGITS_STR)
                END
              ELSE NULL
            END,
            CASE WHEN COALESCE(CHANGED_AT, CREATED_AT) IS NOT NULL THEN YEAR(COALESCE(CHANGED_AT, CREATED_AT)) END
          ) AS ADO_YEAR,

          TRY_TO_NUMBER(ITER_NUM_STR) AS ITERATION_NUM
        FROM j
      )
      SELECT
        FEATURE_ID,
        TITLE,
        STATE,
        TEAM_RAW,
        TEAMID,
        TEAMNAME,
        APP_NAME_RAW,
        EFFORT_POINTS,
        ITERATION_PATH,
        CREATED_AT,
        CHANGED_AT,

        ADO_YEAR,
        ITERATION_NUM,
        CASE
          WHEN ADO_YEAR IS NOT NULL AND ITERATION_NUM IS NOT NULL
            THEN ADO_YEAR::STRING || '-I' || ITERATION_NUM::STRING
          ELSE NULL
        END AS ADO_PI_KEY,

        DELIVERY_TEAM_FTE,
        CONTRACTOR_CS_FTE,
        CONTRACTOR_C_FTE,
        TEAMFTE,
        XOM_RATE,
        CONTRACTOR_CS_RATE,
        CONTRACTOR_C_RATE,

        CAST(DELIVERY_TEAM_FTE + CONTRACTOR_CS_FTE + CONTRACTOR_C_FTE AS FLOAT) AS COMP_DENOM,

        CAST(TEAMFTE * XOM_RATE / 4 AS FLOAT) AS TEAM_COST_PERPI,

        CAST(
          CASE WHEN (DELIVERY_TEAM_FTE + CONTRACTOR_CS_FTE + CONTRACTOR_C_FTE) = 0 THEN 0
               ELSE (DELIVERY_TEAM_FTE / (DELIVERY_TEAM_FTE + CONTRACTOR_CS_FTE + CONTRACTOR_C_FTE))
          END * EFFORT_POINTS * XOM_RATE AS FLOAT
        ) AS DEL_TEAM_COST_PERPI,

        CAST(
          CASE WHEN (DELIVERY_TEAM_FTE + CONTRACTOR_CS_FTE + CONTRACTOR_C_FTE) = 0 THEN 0
               ELSE (CONTRACTOR_CS_FTE / (DELIVERY_TEAM_FTE + CONTRACTOR_CS_FTE + CONTRACTOR_C_FTE))
          END * EFFORT_POINTS * CONTRACTOR_CS_RATE AS FLOAT
        ) AS TEAM_CONTRACTOR_CS_COST_PERPI,

        CAST(
          CASE WHEN (CONTRACTOR_C_FTE + CONTRACTOR_CS_FTE + DELIVERY_TEAM_FTE) = 0 THEN 0
               ELSE (CONTRACTOR_C_FTE / (DELIVERY_TEAM_FTE + CONTRACTOR_CS_FTE + CONTRACTOR_C_FTE))
          END * EFFORT_POINTS * CONTRACTOR_C_RATE AS FLOAT
        ) AS TEAM_CONTRACTOR_C_COST_PERPI

      FROM parsed
    """)


def list_views(pattern: Optional[str] = None) -> pd.DataFrame:
    """List views in the current database/schema."""
    db, sch = _db_and_schema()
    if pattern:
        return fetch_df(f"""
            SELECT TABLE_CATALOG AS DATABASE_NAME,
                   TABLE_SCHEMA  AS SCHEMA_NAME,
                   TABLE_NAME    AS VIEW_NAME
            FROM {db}.INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME LIKE %s
            ORDER BY TABLE_NAME
        """, (sch, pattern))
    else:
        return fetch_df(f"""
            SELECT TABLE_CATALOG AS DATABASE_NAME,
                   TABLE_SCHEMA  AS SCHEMA_NAME,
                   TABLE_NAME    AS VIEW_NAME
            FROM {db}.INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME
        """, (sch,))


def drop_view(view_name: str) -> None:
    """Drop a single view by name (in current DB/schema)."""
    db, sch = _db_and_schema()
    execute(f'DROP VIEW IF EXISTS {db}.{sch}."{view_name}"')


def drop_views_by_prefix(prefix: str = "V_") -> List[str]:
    """Drop all views whose name begins with the given prefix."""
    like = prefix.replace("_", "\\_") + "%"
    df = list_views(pattern=like)
    dropped: List[str] = []
    for _, r in df.iterrows():
        vname = str(r["VIEW_NAME"])
        try:
            drop_view(vname)
            dropped.append(vname)
        except Exception:
            pass
    return dropped


# =========================================================
# Column admin helpers
# =========================================================

def drop_column(table: str, column: str) -> None:
    """Drop a column from a table (idempotent safe call)."""
    db, sch = _db_and_schema()
    try:
        execute(f'ALTER TABLE {db}.{sch}.{table} DROP COLUMN "{column}"')
    except Exception:
        pass

def rename_column(table: str, old: str, new: str) -> None:
    """Rename a column on a table."""
    db, sch = _db_and_schema()
    execute(f'ALTER TABLE {db}.{sch}.{table} RENAME COLUMN "{old}" TO "{new}"')


# =========================================================
# One-time init per session (ORDER MATTERS)
# =========================================================

if not st.session_state.get("_tco_db_init_done"):
    ensure_tables()
    # Sanitize any legacy string FTEs like '5,0' -> 5.00 before enforcing numeric
    repair_team_fte_values()            # <<< ADDED
    normalize_team_numeric_types()
    ensure_team_calc_table()

    # Ensure ADO raw + mapping tables exist BEFORE the cost view
    ensure_ado_minimal_tables()

    # Clean/normalize EFFORT_POINTS once per session (idempotent and fast)
    repair_ado_effort_points_precision()

    # Now the view can safely reference decimals and sanitized FTEs
    ensure_team_cost_view()

    st.session_state["_tco_db_init_done"] = True
