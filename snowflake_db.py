import os
import streamlit as st
import snowflake.connector

# Try to load a local .env if python-dotenv is installed (optional convenience)
try:
    from dotenv import load_dotenv
    load_dotenv()  # loads variables from a .env file into os.environ if present
except Exception:
    pass

def _safe_get_secrets() -> dict:
    """Return st.secrets['snowflake'] as a plain dict if available; otherwise {} (no crash)."""
    try:
        cfg = st.secrets.get("snowflake", {})  # type: ignore[attr-defined]
        return dict(cfg) if isinstance(cfg, dict) else {}
    except Exception:
        return {}

def _get_conn():
    """
    Build a Snowflake connection using (in order of precedence):
    1) st.secrets['snowflake'] (if present)
    2) Environment variables SNOWFLAKE_*
    3) If still missing, raise a readable error listing what is missing
    """
    cfg = _safe_get_secrets()

    user = cfg.get("user") or os.getenv("SNOWFLAKE_USER")
    password = cfg.get("password") or os.getenv("SNOWFLAKE_PASSWORD")
    account = cfg.get("account") or os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = cfg.get("warehouse") or os.getenv("SNOWFLAKE_WAREHOUSE")
    database = cfg.get("database") or os.getenv("SNOWFLAKE_DATABASE")
    schema = cfg.get("schema") or os.getenv("SNOWFLAKE_SCHEMA") or "PUBLIC"
    role = cfg.get("role") or os.getenv("SNOWFLAKE_ROLE")

    missing = [k for k, v in {
        "SNOWFLAKE_USER": user,
        "SNOWFLAKE_PASSWORD": password,
        "SNOWFLAKE_ACCOUNT": account,
        "SNOWFLAKE_WAREHOUSE": warehouse,
        "SNOWFLAKE_DATABASE": database,
        "SNOWFLAKE_SCHEMA": schema,
    }.items() if not v]

    if missing:
        raise RuntimeError(
            "Missing Snowflake credentials.\n"
            "Provide either a .streamlit/secrets.toml with a [snowflake] block, "
            "or set environment variables. Missing: " + ", ".join(missing)
        )

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role
    )
    return conn

def execute(sql: str, params=None):
    """Execute a statement (INSERT/UPDATE/DDL)."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        conn.commit()
    finally:
        conn.close()

def fetch_df(sql: str, params=None):
    """Run a SELECT and return a pandas DataFrame."""
    import pandas as pd
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        cols = [c[0] for c in cur.description] if cur.description else []
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()

def ensure_tables():
    ddl = """
    CREATE TABLE IF NOT EXISTS PROGRAMS (
      PROGRAMID STRING PRIMARY KEY,
      PROGRAMNAME STRING,
      PROGRAMOWNER STRING,
      PROGRAMFTE NUMBER(18,2) DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS TEAMS (
      TEAMID STRING PRIMARY KEY,
      TEAMNAME STRING,
      PROGRAMID STRING,
      COSTPERFTE NUMBER(18,2),
      TEAMFTE NUMBER(18,2) DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS INVOICES (
      INVOICEID STRING PRIMARY KEY,
      APPLICATIONID STRING,
      TEAMID STRING,
      INVOICEDATE DATE,
      RENEWALDATE DATE,
      AMOUNT NUMBER(18,2),
      STATUS STRING,
      VENDOR STRING
    );
    CREATE TABLE IF NOT EXISTS VENDORS (
      VENDORID STRING PRIMARY KEY,
      VENDORNAME STRING
    );
    """
    for stmt in ddl.split(";"):
        s = stmt.strip()
        if s:
            execute(s)

# -----------------------
# Upserts / Deletes
# -----------------------
def upsert_program(program_id: str, name: str, owner: str = None, program_fte: float = 0.0):
    sql = """
    MERGE INTO PROGRAMS t
    USING (
        SELECT %s AS PROGRAMID, %s AS PROGRAMNAME, %s AS PROGRAMOWNER, %s AS PROGRAMFTE
    ) s
    ON t.PROGRAMID = s.PROGRAMID
    WHEN MATCHED THEN UPDATE SET
        PROGRAMNAME = s.PROGRAMNAME,
        PROGRAMOWNER = s.PROGRAMOWNER,
        PROGRAMFTE = s.PROGRAMFTE
    WHEN NOT MATCHED THEN INSERT (PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE)
    VALUES (s.PROGRAMID, s.PROGRAMNAME, s.PROGRAMOWNER, s.PROGRAMFTE);
    """
    execute(sql, (program_id, name, owner, program_fte))

def delete_program(program_id: str):
    execute("DELETE FROM TEAMS WHERE PROGRAMID = %s;", (program_id,))
    execute("DELETE FROM PROGRAMS WHERE PROGRAMID = %s;", (program_id,))

def upsert_team(team_id: str, team_name: str, program_id: str, cost_per_fte: float = 0.0, team_fte: float = 0.0):
    """
    Upsert a team, including TEAMFTE persistence.
    """
    sql = """
    MERGE INTO TEAMS t
    USING (
        SELECT %s AS TEAMID, %s AS TEAMNAME, %s AS PROGRAMID, %s AS COSTPERFTE, %s AS TEAMFTE
    ) s
    ON t.TEAMID = s.TEAMID
    WHEN MATCHED THEN UPDATE SET
        TEAMNAME   = s.TEAMNAME,
        PROGRAMID  = s.PROGRAMID,
        COSTPERFTE = s.COSTPERFTE,
        TEAMFTE    = s.TEAMFTE
    WHEN NOT MATCHED THEN INSERT (TEAMID, TEAMNAME, PROGRAMID, COSTPERFTE, TEAMFTE)
    VALUES (s.TEAMID, s.TEAMNAME, s.PROGRAMID, s.COSTPERFTE, s.TEAMFTE);
    """
    execute(sql, (team_id, team_name, program_id, cost_per_fte, team_fte))

def delete_team(team_id: str):
    execute("DELETE FROM TEAMS WHERE TEAMID = %s;", (team_id,))

def upsert_invoice(invoice_id: str, application_id: str, team_id: str, invoice_date, renewal_date, amount: float, status: str, vendor: str):
    sql = """
    MERGE INTO INVOICES t
    USING (
        SELECT %s AS INVOICEID, %s AS APPLICATIONID, %s AS TEAMID, %s AS INVOICEDATE, %s AS RENEWALDATE,
               %s AS AMOUNT, %s AS STATUS, %s AS VENDOR
    ) s
    ON t.INVOICEID = s.INVOICEID
    WHEN MATCHED THEN UPDATE SET
        APPLICATIONID = s.APPLICATIONID, TEAMID = s.TEAMID, INVOICEDATE = s.INVOICEDATE, RENEWALDATE = s.RENEWALDATE,
        AMOUNT = s.AMOUNT, STATUS = s.STATUS, VENDOR = s.VENDOR
    WHEN NOT MATCHED THEN INSERT (INVOICEID, APPLICATIONID, TEAMID, INVOICEDATE, RENEWALDATE, AMOUNT, STATUS, VENDOR)
    VALUES (s.INVOICEID, s.APPLICATIONID, s.TEAMID, s.INVOICEDATE, s.RENEWALDATE, s.AMOUNT, s.STATUS, s.VENDOR);
    """
    execute(sql, (invoice_id, application_id, team_id, invoice_date, renewal_date, amount, status, vendor))

def delete_invoice(invoice_id: str):
    execute("DELETE FROM INVOICES WHERE INVOICEID = %s;", (invoice_id,))

def upsert_vendor(vendor_id: str, vendor_name: str):
    sql = """
    MERGE INTO VENDORS t
    USING (SELECT %s AS VENDORID, %s AS VENDORNAME) s
    ON t.VENDORID = s.VENDORID
    WHEN MATCHED THEN UPDATE SET VENDORNAME = s.VENDORNAME
    WHEN NOT MATCHED THEN INSERT (VENDORID, VENDORNAME)
    VALUES (s.VENDORID, s.VENDORNAME);
    """
    execute(sql, (vendor_id, vendor_name))

def delete_vendor(vendor_id: str):
    execute("DELETE FROM VENDORS WHERE VENDORID = %s;", (vendor_id,))

def ensure_programs_schema():
    # Safe/idempotent: adds column only if missing
    execute("ALTER TABLE IF EXISTS PROGRAMS ADD COLUMN IF NOT EXISTS PROGRAMFTE NUMBER(18,2) DEFAULT 0;")

def ensure_teams_schema():
    execute("ALTER TABLE IF EXISTS TEAMS ADD COLUMN IF NOT EXISTS TEAMFTE NUMBER(18,2) DEFAULT 0;")
