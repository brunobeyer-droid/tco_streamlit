# Edit.py — Advanced admin page (danger zone)
import uuid
import streamlit as st
import datetime as dt
from typing import Optional, Tuple

from snowflake_db import (
    ensure_tables as _ensure_tables,
    fetch_df, execute,
    upsert_program,   delete_program,
    upsert_team,      delete_team,
    upsert_vendor,    delete_vendor,
    # upsert_application,  # not exported by snowflake_db; not used in this page
    delete_application,
    # upsert_invoice,      # imported originally but not used in this page
    # rollover_year, list_rollovers, rollback_rollover,  # provide local helpers below
    _read_snowflake_secrets, _fq,  # internal helpers to build FQ table names
)

# Optional sidebar helper; fall back safely if not present
try:
    from utils.sidebar import render_global_actions
except Exception:
    def render_global_actions():
        pass

st.set_page_config(page_title="Bulk Edit (Advanced)", page_icon="✏️", layout="wide")
render_global_actions()

# Ensure schema once per session
if "_tco_init" not in st.session_state:
    _ensure_tables()
    st.session_state["_tco_init"] = True

# ----------------------------
# Local helpers for rollover
# ----------------------------
def _db() -> Tuple[str, str]:
    cfg = _read_snowflake_secrets()
    return cfg.get("database", "TCODB"), cfg.get("schema", "PUBLIC")

def rollover_year(from_year: int, to_year: int, created_by: Optional[str] = None) -> Tuple[str, int]:
    """
    Create 'Planned' invoices for `to_year` by copying rows from `from_year`.

    CHANGES:
    - Rolls over ONLY Recurring invoices:
      COALESCE(i.INVOICE_TYPE,'Recurring Invoice') = 'Recurring Invoice'
    - Sets INVOICE_TYPE on new rows to 'Recurring Invoice'
    - Uses COALESCE(AMOUNT_NEXT_YEAR, AMOUNT) and shifts RENEWALDATE by year delta.
    - Tags new rows with ROLLOVER_BATCH_ID and ROLLED_OVER_FROM_YEAR so we can undo.
    """
    batch_id = str(uuid.uuid4())
    year_delta = int(to_year) - int(from_year)

    # Insert new invoices (Recurring only)
    insert_sql = f"""
        INSERT INTO { _fq('INVOICES') } (
            INVOICEID, APPLICATIONID, TEAMID,
            INVOICEDATE, RENEWALDATE, AMOUNT, STATUS, FISCAL_YEAR,
            PRODUCT_OWNER, AMOUNT_NEXT_YEAR, CONTRACT_ACTIVE,
            COMPANY_CODE, COST_CENTER, SERIAL_NUMBER, WORK_ORDER, AGREEMENT_NUMBER,
            CONTRACT_DUE, SERVICE_TYPE, NOTES,
            GROUPID, PROGRAMID_AT_BOOKING, VENDORID_AT_BOOKING, GROUPID_AT_BOOKING,
            ROLLOVER_BATCH_ID, ROLLED_OVER_FROM_YEAR,
            INVOICE_TYPE
        )
        SELECT
            UUID_STRING(), i.APPLICATIONID, i.TEAMID,
            /* keep INVOICEDATE NULL for planned rows */ NULL,
            /* shift renewal date by year_delta if present */
            CASE WHEN i.RENEWALDATE IS NOT NULL THEN DATEADD(year, {year_delta}, i.RENEWALDATE) ELSE NULL END,
            COALESCE(i.AMOUNT_NEXT_YEAR, i.AMOUNT) AS AMOUNT,
            'Planned' AS STATUS,
            %s AS FISCAL_YEAR,
            i.PRODUCT_OWNER,
            i.AMOUNT_NEXT_YEAR,
            /* planned rows default to contract_active unless explicitly set */ COALESCE(i.CONTRACT_ACTIVE, TRUE),
            i.COMPANY_CODE, i.COST_CENTER, i.SERIAL_NUMBER, i.WORK_ORDER, i.AGREEMENT_NUMBER,
            i.CONTRACT_DUE, i.SERVICE_TYPE, i.NOTES,
            i.GROUPID, i.PROGRAMID_AT_BOOKING, i.VENDORID_AT_BOOKING, i.GROUPID_AT_BOOKING,
            %s AS ROLLOVER_BATCH_ID,
            %s AS ROLLED_OVER_FROM_YEAR,
            /* ensure the new rows are explicitly marked as Recurring */
            'Recurring Invoice' AS INVOICE_TYPE
        FROM { _fq('INVOICES') } i
        WHERE i.FISCAL_YEAR = %s
          AND COALESCE(i.INVOICE_TYPE, 'Recurring Invoice') = 'Recurring Invoice'
    """
    # FISCAL_YEAR (to), BATCH, FROM_YEAR, FROM_YEAR
    execute(insert_sql, (int(to_year), batch_id, int(from_year), int(from_year)))

    # Count inserted
    cnt_df = fetch_df(f"SELECT COUNT(*) AS CNT FROM { _fq('INVOICES') } WHERE ROLLOVER_BATCH_ID = %s", (batch_id,))
    inserted = int(cnt_df.iloc[0]["CNT"]) if not cnt_df.empty else 0

    # Log the batch
    log_sql = f"""
        INSERT INTO { _fq('ROLLOVER_LOG') } (BATCH_ID, FROM_YEAR, TO_YEAR, ROWS_INSERTED, CREATED_BY)
        VALUES (%s, %s, %s, %s, %s)
    """
    execute(log_sql, (batch_id, int(from_year), int(to_year), inserted, created_by))
    return batch_id, inserted

def list_rollovers():
    return fetch_df(f"""
        SELECT BATCH_ID, FROM_YEAR, TO_YEAR, ROWS_INSERTED, CREATED_AT, CREATED_BY
        FROM { _fq('ROLLOVER_LOG') }
        ORDER BY CREATED_AT DESC
    """)

def rollback_rollover(batch_id: str) -> int:
    """
    Delete invoices created by a specific rollover batch.
    (No change needed for invoice types—batch id targets only the rows created by rollover,
    which we now ensure are Recurring.)
    """
    # How many rows will we delete?
    cnt_df = fetch_df(f"SELECT COUNT(*) AS CNT FROM { _fq('INVOICES') } WHERE ROLLOVER_BATCH_ID = %s", (batch_id,))
    to_delete = int(cnt_df.iloc[0]["CNT"]) if not cnt_df.empty else 0
    # Delete invoices first, then the log entry
    execute(f"DELETE FROM { _fq('INVOICES') } WHERE ROLLOVER_BATCH_ID = %s", (batch_id,))
    execute(f"DELETE FROM { _fq('ROLLOVER_LOG') } WHERE BATCH_ID = %s", (batch_id,))
    return to_delete

# ----------------------------
# Utility: safe table viewer
# ----------------------------
def _table_exists(table: str) -> bool:
    db, sch = _db()
    q = f"""
      SELECT 1
      FROM {db}.INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = %s AND UPPER(TABLE_NAME) = UPPER(%s)
      LIMIT 1
    """
    return not fetch_df(q, (sch, table)).empty

def _view_table(sql: str, *, title: str, height: int = 280):
    try:
        df = fetch_df(sql)
        st.markdown(f"**{title}**")
        if df is None or df.empty:
            st.info(f"No rows in {title}.")
        else:
            st.dataframe(df, use_container_width=True, height=height)
    except Exception as e:
        st.warning(f"{title} unavailable: {e}")

# ----------------------------
# UI
# ----------------------------
st.title("✏️ Bulk Edit (Advanced)")
st.caption("Administrator workspace for high-impact actions. Use with care.")

# Tabs for clarity
tab_browse, tab_rollover, tab_rollback, tab_delete, tab_schema, tab_sql = st.tabs(
    ["Browse Data", "Rollover", "Rollback", "Deletions", "Schema & Dependencies", "Raw SQL"]
)

# =========================================
# BROWSE DATA
# =========================================
with tab_browse:
    st.subheader("Quick Viewers (read-only)")

    # Top grid: core entities
    colA, colB = st.columns(2)
    with colA:
        _view_table("SELECT PROGRAMID, PROGRAMNAME, PROGRAMOWNER, PROGRAMFTE FROM PROGRAMS ORDER BY PROGRAMNAME",
                    title="Programs")
    with colB:
        _view_table("SELECT TEAMID, TEAMNAME, PROGRAMID, TEAMFTE, COSTPERFTE, PRODUCTOWNER FROM TEAMS ORDER BY TEAMNAME",
                    title="Teams")

    colC, colD = st.columns(2)
    with colC:
        _view_table("SELECT VENDORID, VENDORNAME FROM VENDORS ORDER BY VENDORNAME", title="Vendors")
    with colD:
        _view_table(
            """
            SELECT
              A.APPLICATIONID,
              A.APPLICATIONNAME,
              A.ADD_INFO,
              A.VENDORID,
              V.VENDORNAME,
              A.GROUPID
            FROM APPLICATIONS A
            LEFT JOIN VENDORS V ON V.VENDORID = A.VENDORID
            ORDER BY A.APPLICATIONNAME
            """,
            title="Applications"
        )

    # Middle grid: groups & legacy link table (if present)
    colE, colF = st.columns(2)
    with colE:
        _view_table(
            """
            SELECT
              g.GROUPID,
              g.GROUPNAME,
              g.TEAMID,
              t.TEAMNAME,
              g.PROGRAMID,
              p.PROGRAMNAME,
              g.DEFAULT_VENDORID AS VENDORID,
              v.VENDORNAME,
              g.OWNER,
              g.CREATED_AT
            FROM APPLICATION_GROUPS g
            LEFT JOIN TEAMS t    ON t.TEAMID    = g.TEAMID
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = g.PROGRAMID
            LEFT JOIN VENDORS v  ON v.VENDORID  = g.DEFAULT_VENDORID
            ORDER BY g.GROUPNAME
            """,
            title="Application Groups"
        )
    with colF:
        if _table_exists("TEAMS_APP_GROUPS"):
            _view_table(
                """
                SELECT
                  tag.TEAMID,
                  t.TEAMNAME,
                  tag.GROUPID,
                  g.GROUPNAME
                FROM TEAMS_APP_GROUPS tag
                LEFT JOIN TEAMS t ON t.TEAMID = tag.TEAMID
                LEFT JOIN APPLICATION_GROUPS g ON g.GROUPID = tag.GROUPID
                ORDER BY t.TEAMNAME, g.GROUPNAME
                """,
                title="TEAMS_APP_GROUPS (legacy link)"
            )
        else:
            st.info("Legacy link table TEAMS_APP_GROUPS not found (skipped).")

    st.markdown("---")

    # Invoices (full read-only viewer)
    _view_table(
        """
        SELECT
          i.INVOICEID,
          i.FISCAL_YEAR,
          i.INVOICEDATE,
          i.RENEWALDATE,
          i.AMOUNT,
          i.STATUS,
          COALESCE(i.INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE,
          t.TEAMNAME,
          p.PROGRAMNAME,
          a.APPLICATIONNAME,
          v.VENDORNAME
        FROM INVOICES i
        LEFT JOIN TEAMS t ON t.TEAMID = i.TEAMID
        LEFT JOIN PROGRAMS p ON p.PROGRAMID = COALESCE(i.PROGRAMID_AT_BOOKING, t.PROGRAMID)
        LEFT JOIN APPLICATIONS a ON a.APPLICATIONID = i.APPLICATIONID
        LEFT JOIN VENDORS v ON v.VENDORID = COALESCE(i.VENDORID_AT_BOOKING, a.VENDORID)
        ORDER BY i.INVOICEDATE DESC
        """,
        title="Invoices",
        height=340
    )

    st.markdown("---")

    # Bottom grid: notes, attachments, rollover log
    colG, colH = st.columns(2)
    with colG:
        _view_table(
            """
            SELECT NOTE_ID, INVOICEID, NOTE_TEXT, CREATED_AT, CREATED_BY
            FROM INVOICE_NOTES
            ORDER BY CREATED_AT DESC
            """,
            title="Invoice Notes"
        )
    with colH:
        _view_table(
            """
            SELECT ATTACHMENT_ID, INVOICEID, FILENAME, MIMETYPE, UPLOADED_AT
            FROM INVOICE_ATTACHMENTS
            ORDER BY UPLOADED_AT DESC
            """,
            title="Invoice Attachments"
        )

    st.markdown("---")

    _view_table(
        """
        SELECT BATCH_ID, FROM_YEAR, TO_YEAR, ROWS_INSERTED, CREATED_AT, CREATED_BY
        FROM ROLLOVER_LOG
        ORDER BY CREATED_AT DESC
        """,
        title="Rollover Log"
    )

# =========================================
# ROLLOVER
# =========================================
with tab_rollover:
    st.subheader("🔁 Rollover Planned Invoices to Next Year")
    st.write(
        "Creates **Planned** invoices for the next fiscal year from last year's values "
        "(uses `AMOUNT_NEXT_YEAR` when present, otherwise `AMOUNT`). "
        "Only **Recurring** invoices are rolled over. "
        "Every rollover is tagged with a **batch id** so it can be undone later."
    )
    this_year = dt.date.today().year
    from_year = st.number_input("From fiscal year", value=this_year, step=1, format="%d")
    to_year   = st.number_input("To fiscal year",   value=this_year + 1, step=1, format="%d")
    created_by = st.text_input("Created by (optional)", value="streamlit")
    if st.button("Generate next year's planned invoices", type="primary"):
        if to_year <= from_year:
            st.error("`To fiscal year` must be greater than `From fiscal year`.")
        else:
            batch_id, inserted = rollover_year(int(from_year), int(to_year), created_by=created_by or None)
            st.success(f"Batch {batch_id[:8]}… created {inserted} planned recurring invoice(s) for {to_year}.")
            st.caption("You can undo this batch below in the **Rollback** tab.")
            st.cache_data.clear()

# =========================================
# ROLLBACK
# =========================================
with tab_rollback:
    st.subheader("↩️ Undo a Rollover Batch")
    logs = list_rollovers()

    # Manual rollback (works even if no batches show up below)
    st.markdown("**Manual rollback by Batch ID**")
    mcol1, mcol2 = st.columns([3, 1])
    with mcol1:
        manual_batch_id = st.text_input("Enter Batch ID (paste full ID)")
    with mcol2:
        if st.button("Rollback (Manual)"):
            if not manual_batch_id.strip():
                st.error("Please paste a Batch ID.")
            else:
                deleted = rollback_rollover(manual_batch_id.strip())
                st.success(f"Rolled back {deleted} invoice(s) from batch {manual_batch_id[:8]}…")
                st.cache_data.clear()

    st.markdown("---")
    st.markdown("**Rollback from recent batches**")
    if logs.empty:
        st.info("No rollover batches found yet. Run a rollover first, or use the manual rollback above.")
    else:
        logs = logs.copy()
        logs["LABEL"] = logs.apply(
            lambda r: f"{str(r['BATCH_ID'])[:8]}…  |  {int(r['FROM_YEAR'])} → {int(r['TO_YEAR'])}  |  {int(r['ROWS_INSERTED'])} rows  |  {r['CREATED_AT']}",
            axis=1
        )
        pick = st.selectbox("Rollover Batches", logs["LABEL"].tolist())
        sel = logs.loc[logs["LABEL"] == pick].iloc[0]
        batch_id = sel["BATCH_ID"]

        # Preview rows in this batch
        preview = fetch_df("SELECT * FROM INVOICES WHERE ROLLOVER_BATCH_ID = %s ORDER BY INVOICEDATE", (batch_id,))
        if preview.empty:
            st.warning("No invoices found for this batch (maybe already rolled back).")
        else:
            st.dataframe(preview, use_container_width=True, height=320)

        st.warning("Rollback deletes ONLY invoices created by this batch. Other data is untouched.")
        if st.button("⚠️ Rollback this selected batch", type="primary"):
            deleted = rollback_rollover(batch_id)
            st.success(f"Rolled back {deleted} invoice(s) from batch {batch_id[:8]}…")
            st.cache_data.clear()

# =========================================
# DELETIONS
# =========================================
with tab_delete:
    st.subheader("Dangerous Operations")
    st.caption("These actions permanently remove data. Use with extreme care.")

    st.markdown("### 🗑️ Delete Program (and its child Teams & Invoices)")
    st.write("Deletes all Invoices under Teams that belong to the Program, then deletes those Teams, then the Program.")
    prog_id = st.text_input("ProgramID to delete", key="del_prog_id")
    if st.button("Delete Program & children", key="del_prog_btn"):
        if not prog_id.strip():
            st.error("Please provide a ProgramID.")
        else:
            teams = fetch_df("SELECT TEAMID FROM TEAMS WHERE PROGRAMID = %s", (prog_id,))
            for _, tr in teams.iterrows():
                invs = fetch_df("SELECT INVOICEID FROM INVOICES WHERE TEAMID = %s", (tr["TEAMID"],))
                for __, ir in invs.iterrows():
                    execute("DELETE FROM INVOICES WHERE INVOICEID = %s", (ir["INVOICEID"],))
                delete_team(tr["TEAMID"])
            delete_program(prog_id)
            st.success("Program and children deleted.")
            st.cache_data.clear()

    st.markdown("---")
    st.markdown("### 🗑️ Delete Team by TeamID (and its Invoices)")
    st.write("Deletes all Invoices that reference the Team, then deletes the Team record.")
    team_id_to_del = st.text_input("TeamID to delete", key="del_team_id")
    if st.button("Delete Team & its invoices", key="del_team_btn"):
        if not team_id_to_del.strip():
            st.error("Please provide a TeamID.")
        else:
            invs = fetch_df("SELECT INVOICEID FROM INVOICES WHERE TEAMID = %s", (team_id_to_del,))
            for _, ir in invs.iterrows():
                execute("DELETE FROM INVOICES WHERE INVOICEID = %s", (ir["INVOICEID"],))
            delete_team(team_id_to_del)
            st.success("Team and its invoices deleted.")
            st.cache_data.clear()

    st.markdown("---")
    st.markdown("### 🗑️ Delete Application (and its Invoices)")
    st.write("Deletes all Invoices that reference the Application, then deletes the Application.")
    app_id = st.text_input("ApplicationID to delete", key="del_app_id")
    if st.button("Delete Application & its invoices", key="del_app_btn"):
        if not app_id.strip():
            st.error("Please provide an ApplicationID.")
        else:
            invs = fetch_df("SELECT INVOICEID FROM INVOICES WHERE APPLICATIONID = %s", (app_id,))
            for _, ir in invs.iterrows():
                execute("DELETE FROM INVOICES WHERE INVOICEID = %s", (ir["INVOICEID"],))
            delete_application(app_id)
            st.success("Application and invoices deleted.")
            st.cache_data.clear()

    st.markdown("---")
    st.markdown("### 🗑️ Delete Vendor (and orphan Applications)")
    st.write("Deletes Applications for this Vendor only if they have no Invoices; then deletes the Vendor.")
    vend_id = st.text_input("VendorID to delete", key="del_vendor_id")
    if st.button("Delete Vendor & orphan applications", key="del_vendor_btn"):
        if not vend_id.strip():
            st.error("Please provide a VendorID.")
        else:
            apps = fetch_df("""
                SELECT a.APPLICATIONID
                FROM APPLICATIONS a
                LEFT JOIN INVOICES i ON i.APPLICATIONID = a.APPLICATIONID
                WHERE a.VENDORID = %s AND i.INVOICEID IS NULL
            """, (vend_id,))
            for _, ar in apps.iterrows():
                delete_application(ar["APPLICATIONID"])
            delete_vendor(vend_id)
            st.success("Vendor and orphan applications deleted.")
            st.cache_data.clear()

    st.markdown("---")
    st.markdown("### 🗑️ Delete Invoice by ID")
    st.write("Preview the invoice, then delete by **INVOICEID**.")
    inv_id = st.text_input("InvoiceID to preview/delete", key="del_invoice_id")

    preview = None
    if inv_id.strip():
        preview = fetch_df("""
            SELECT
              i.INVOICEID,
              i.FISCAL_YEAR,
              i.INVOICEDATE,
              i.RENEWALDATE,
              i.AMOUNT,
              i.STATUS,
              COALESCE(i.INVOICE_TYPE,'Recurring Invoice') AS INVOICE_TYPE,
              t.TEAMNAME,
              p.PROGRAMNAME,
              a.APPLICATIONNAME,
              v.VENDORNAME
            FROM INVOICES i
            LEFT JOIN TEAMS t ON t.TEAMID = i.TEAMID
            LEFT JOIN PROGRAMS p ON p.PROGRAMID = COALESCE(i.PROGRAMID_AT_BOOKING, t.PROGRAMID)
            LEFT JOIN APPLICATIONS a ON a.APPLICATIONID = i.APPLICATIONID
            LEFT JOIN VENDORS v ON v.VENDORID = COALESCE(i.VENDORID_AT_BOOKING, a.VENDORID)
            WHERE i.INVOICEID = %s
        """, (inv_id.strip(),))

    if preview is not None and not preview.empty:
        st.dataframe(preview, use_container_width=True)
        if st.button("Delete This Invoice", key="btn_delete_invoice", type="secondary"):
            execute("DELETE FROM INVOICES WHERE INVOICEID = %s", (inv_id.strip(),))
            st.success("Invoice deleted.")
            st.cache_data.clear()
    elif inv_id.strip():
        st.warning("Invoice not found. Please check the ID and try again.")

# =========================================
# SCHEMA & DEPENDENCIES (diagram)
# =========================================
with tab_schema:
    st.subheader("Schema & Dependencies")
    st.caption("High-level view of core tables, relationships, and admin-side deletion flows (hover nodes/edges for details).")

    st.markdown(
        """
**Legend**
- **Solid arrows** = parent → child (FK points from child to parent).
- **Dashed arrows** = lookup/default relationships (non-mandatory / denormalized use).
- **Node colors**:
  - 🔵 Programs/Teams = Organizational entities (moderate risk)
  - 🟠 Groups/Applications = App catalog entities (moderate risk)
  - 🟢 Vendors = Catalog lookups (lower risk)
  - 🩷 Invoices = Financial data (high risk)
  - 🟩 Notes/Attachments/Logs = supporting artifacts
- **Admin deletion flows (in this page)**:
  - *Delete Program* → deletes Teams’ **Invoices** first (by Team), then **Teams**, then the **Program**.
  - *Delete Team* → deletes **Invoices** tied to that Team, then deletes the **Team**.
  - *Delete Application* → allowed only if **no Invoices** exist.
  - *Delete Group* → allowed only if **no Application Instances** exist.
  - *Delete Vendor* → removes only **orphan Applications** with no invoices here; otherwise blocked.
- **Rollover** → inserts new **Invoices** tagged with `ROLLOVER_BATCH_ID` and logs the batch in **ROLLOVER_LOG**.
        """
    )

    # Graphviz with tooltips & risk colors
    dot = r"""
digraph G {
  graph [rankdir=LR, nodesep=0.6, ranksep=0.6, tooltip="Schema dependencies"];
  node  [shape=record, fontsize=10, style="rounded,filled", color="#CCCCCC"];
  edge  [color="#888888", arrowsize=0.8];

  # Colors by risk
  # Org (Programs/Teams): blue, App Catalog (Groups/Apps): orange, Vendors: green, Invoices: pink, Support: light green
  Programs   [label="{PROGRAMS|PROGRAMID (PK)\lPROGRAMNAME (unique)\lPROGRAMOWNER\lPROGRAMFTE}", fillcolor="#E8F3FF", tooltip="Programs. Unique PROGRAMNAME. Deleting a Program in this page cascades: delete Teams' Invoices, Teams, then Program."];
  Teams      [label="{TEAMS|TEAMID (PK)\lTEAMNAME (unique)\lPROGRAMID (FK → PROGRAMS)\lTEAMFTE\lDELIVERY_TEAM_FTE\lCONTRACTOR_C_FTE\lCONTRACTOR_CS\lPRODUCTOWNER}", fillcolor="#E8F3FF", tooltip="Teams belong to Programs. Unique TEAMNAME. Delete flow removes invoices referencing this Team first."];
  Groups     [label="{APPLICATION_GROUPS|GROUPID (PK)\lGROUPNAME (unique)\lTEAMID (FK → TEAMS)\lPROGRAMID (denorm)\lDEFAULT_VENDORID (FK → VENDORS)\lOWNER\lCREATED_AT}", fillcolor="#FFF2DC", tooltip="Application groups owned by Teams. Delete allowed only if no application instances exist."];
  Vendors    [label="{VENDORS|VENDORID (PK)\lVENDORNAME (unique)}", fillcolor="#EAF8F0", tooltip="Vendors. Unique VENDORNAME. In this page, only orphan Applications (no invoices) are deleted with the Vendor."];
  Apps       [label="{APPLICATIONS|APPLICATIONID (PK)\lAPPLICATIONNAME (unique)\lGROUPID (FK → APPLICATION_GROUPS)\lADD_INFO\lVENDORID (FK → VENDORS)}", fillcolor="#FFF2DC", tooltip="Application instances (canonical name is often Group - Instance). Can only be deleted if no invoices exist."];
  Invoices   [label="{INVOICES|INVOICEID (PK)\lAPPLICATIONID (FK → APPLICATIONS)\lTEAMID (FK → TEAMS)\lFISCAL_YEAR\lRENEWALDATE\lAMOUNT\lSTATUS\lINVOICE_TYPE\lAMOUNT_NEXT_YEAR\lCONTRACT_ACTIVE\lCOMPANY_CODE\lCOST_CENTER\lSERIAL_NUMBER\lWORK_ORDER\lAGREEMENT_NUMBER\lCONTRACT_DUE\lSERVICE_TYPE\lNOTES\lPROGRAMID_AT_BOOKING\lVENDORID_AT_BOOKING\lGROUPID\lGROUPID_AT_BOOKING\lROLLOVER_BATCH_ID\lROLLED_OVER_FROM_YEAR}", fillcolor="#FFEFF3", tooltip="Financial records. High risk. Rollover writes planned recurring invoices; rollback removes batch-tagged rows."];

  RolloverLog [label="{ROLLOVER_LOG|BATCH_ID (PK)\lFROM_YEAR\lTO_YEAR\lROWS_INSERTED\lCREATED_AT\lCREATED_BY}", fillcolor="#F0F6FF", tooltip="Log of rollover batches for audit & rollback."];
  Notes       [label="{INVOICE_NOTES|NOTE_ID (PK)\lINVOICEID (FK → INVOICES)\lNOTE_TEXT\lCREATED_AT\lCREATED_BY}", fillcolor="#F7FFF3", tooltip="Historical notes per invoice."];
  Atts        [label="{INVOICE_ATTACHMENTS|ATTACHMENT_ID (PK)\lINVOICEID (FK → INVOICES)\lFILENAME\MIMETYPE\lCONTENT\lUPLOADED_AT}", fillcolor="#F7FFF3", tooltip="Binary attachments stored per invoice."];

  # Legacy / optional link table (if present historically)
  TAG         [label="{TEAMS_APP_GROUPS (legacy)|TEAMID (FK → TEAMS)\lGROUPID (FK → APPLICATION_GROUPS)}", fillcolor="#FFF9E8", tooltip="Legacy many-to-many link between teams and groups (historical). Not used when APPLICATION_GROUPS.TEAMID is present."];

  # --- Core FK relationships (solid) ---
  Programs -> Teams [tooltip="PROGRAMS (1) → TEAMS (many)"];
  Teams    -> Groups [tooltip="TEAMS (1) → APPLICATION_GROUPS (many)"];
  Groups   -> Apps [tooltip="APPLICATION_GROUPS (1) → APPLICATIONS (many)"];
  Apps     -> Invoices [tooltip="APPLICATIONS (1) → INVOICES (many)"];
  Teams    -> Invoices [tooltip="TEAMS (1) → INVOICES (many)"];

  # --- Lookups / defaults (dashed) ---
  Vendors  -> Groups  [style=dashed, label="DEFAULT_VENDORID", tooltip="Groups may carry a default vendor (optional)."];
  Vendors  -> Apps    [style=dashed, label="VENDORID", tooltip="Application instances may reference a vendor (optional)."];
  RolloverLog -> Invoices [style=dashed, label="ROLLOVER_BATCH_ID", tooltip="Invoices created by a batch are tagged with this ID for rollback."];

  # --- Child-of-Invoice (solid) ---
  Invoices -> Notes [tooltip="INVOICES (1) → INVOICE_NOTES (many)"];
  Invoices -> Atts  [tooltip="INVOICES (1) → INVOICE_ATTACHMENTS (many)"];

  # --- Legacy link table (solid) ---
  Teams  -> TAG [tooltip="Legacy mapping: Team → Group"];
  Groups -> TAG [tooltip="Legacy mapping: Group → Team"];

  # --- Ranks for tidy layout ---
  {rank=same; Programs; Vendors;}
  {rank=same; Groups; TAG;}
  {rank=same; Apps; RolloverLog;}
  {rank=same; Invoices; Notes; Atts;}
}
"""
    st.graphviz_chart(dot, use_container_width=True)

# =========================================
# RAW SQL (read-only)
# =========================================
with tab_sql:
    st.subheader("Raw SQL (read-only)")
    user_sql = st.text_area(
        "Run a SELECT (for diagnostic reads). Non-SELECT statements are blocked here.",
        value="SELECT CURRENT_USER() AS USER, CURRENT_ROLE() AS ROLE, CURRENT_DATABASE() AS DB, CURRENT_SCHEMA() AS SCHEMA;"
    )
    if st.button("Run SELECT", key="run_sql_btn"):
        sql_up = user_sql.strip().upper()
        if not sql_up.startswith("SELECT"):
            st.error("Only SELECT statements are allowed in this box.")
        else:
            try:
                st.dataframe(fetch_df(user_sql), use_container_width=True)
            except Exception as e:
                st.error(e)
