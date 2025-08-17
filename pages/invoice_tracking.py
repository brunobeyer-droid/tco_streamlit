# filename: pages/invoice_tracking.py
import streamlit as st
import uuid
import datetime as dt

# If you sometimes run this page directly, uncomment:
# import sys, os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from snowflake_db import ensure_tables, fetch_df, upsert_invoice, delete_invoice

st.set_page_config(page_title="Invoices / Licenses", page_icon="🧾", layout="wide")
st.title("🧾 Invoices / License Tracking")

# -------------------------------------------------------------------
# Ensure base tables exist once per session
# -------------------------------------------------------------------
if not st.session_state.get("_init_invoices_done"):
    ensure_tables()
    st.session_state["_init_invoices_done"] = True

# (Optional) keep lightweight attachment info only in session
st.session_state.setdefault("invoice_attachments", {})  # {invoice_id: {"name":..., "data":...}}

# -------------------------------------------------------------------
# Helpers: cached loaders
# -------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def get_teams_df():
    return fetch_df("SELECT TEAMID, TEAMNAME FROM TEAMS ORDER BY TEAMNAME;")

@st.cache_data(show_spinner=False)
def get_invoices_df():
    return fetch_df("""
        SELECT i.INVOICEID,
               t.TEAMNAME,
               i.INVOICEDATE,
               i.RENEWALDATE,
               COALESCE(i.AMOUNT,0) AS AMOUNT,
               i.STATUS,
               i.VENDOR
        FROM INVOICES i
        LEFT JOIN TEAMS t ON t.TEAMID = i.TEAMID
        ORDER BY i.INVOICEDATE DESC, i.INVOICEID DESC;
    """)

@st.cache_data(show_spinner=False)
def get_vendors_df():
    return fetch_df("SELECT VENDORNAME FROM VENDORS ORDER BY UPPER(VENDORNAME);")

def invalidate_invoices_cache():
    get_invoices_df.clear()

# -------------------------------------------------------------------
# Load reference data
# -------------------------------------------------------------------
teams_df = get_teams_df()
team_name_to_id = dict(zip(teams_df["TEAMNAME"], teams_df["TEAMID"])) if not teams_df.empty else {}
team_names = list(team_name_to_id.keys())

if not team_names:
    st.warning("No Teams found. Please create Teams first, then link invoices to a team.")
    st.stop()

# -------------------------------------------------------------------
# Add Invoice (form)
# -------------------------------------------------------------------
st.subheader("Add New License Invoice")

a1, a2, a3 = st.columns([2, 1.2, 1.2])
with a1:
    a_team = st.selectbox("Team", options=team_names, index=0)
with a2:
    a_amount = st.number_input("Cost 2025 (Amount)", min_value=0.0, step=50.0, value=0.0)
with a3:
    a_status = st.selectbox("Status", ["Paid", "Pending", "Overdue"], index=1)

b1, b2, b3 = st.columns([1.2, 1.2, 2])
with b1:
    a_inv_date = st.date_input("Invoice Date", value=dt.date.today())
with b2:
    a_ren_date = st.date_input("Renewal Date", value=dt.date.today())
with b3:
    vdf = get_vendors_df()
vendor_options = vdf["VENDORNAME"].tolist() if not vdf.empty else []
a_vendor = st.selectbox("Vendor", options=vendor_options, index=0 if vendor_options else None)


# Optional extra (not persisted to Snowflake unless you want schema changes)
c1, c2 = st.columns([1.2, 2])
with c1:
    a_amount_2026 = st.number_input("Cost 2026 (not saved to DB)", min_value=0.0, step=50.0, value=0.0)
with c2:
    a_product = st.text_input("Product / Application (optional, not saved)")

# Attachment kept only in session (demo)
uploaded_file = st.file_uploader("Attach File (kept in session only for now)", type=["pdf", "png", "jpg", "jpeg"])

if st.button("Save Invoice", type="primary"):
    try:
        iid = str(uuid.uuid4())
        upsert_invoice(
            invoice_id=iid,
            application_id=None,  # not used for now
            team_id=team_name_to_id[a_team],
            invoice_date=a_inv_date,
            renewal_date=a_ren_date,
            amount=float(a_amount or 0.0),  # Cost 2025 -> AMOUNT
            status=a_status,
            vendor=a_vendor.strip() or None
        )
        # Save attachment metadata in session only (so you can download later)
        if uploaded_file:
            st.session_state["invoice_attachments"][iid] = {
                "name": uploaded_file.name,
                "data": uploaded_file.getvalue(),  # bytes
            }
        invalidate_invoices_cache()
        st.success("Invoice saved.")
        st.rerun()
    except Exception as e:
        st.error(f"Failed to save invoice: {e}")

st.divider()

# -------------------------------------------------------------------
# Current Invoices (inline edit with Update/Delete)
# -------------------------------------------------------------------
st.subheader("Current Invoices")

inv_df = get_invoices_df()
if inv_df.empty:
    st.info("No invoices yet. Add one above.")
else:
    COLS = [2, 1.1, 1.2, 1.2, 1.0, 1.2, 0.9, 0.9]  # Team, Amount, InvDate, RenDate, Status, Vendor, Update, Delete
    h1, h2, h3, h4, h5, h6, h7, h8 = st.columns(COLS, gap="small")
    with h1: st.markdown("**Team**")
    with h2: st.markdown("**Amount**")
    with h3: st.markdown("**Invoice Date**")
    with h4: st.markdown("**Renewal Date**")
    with h5: st.markdown("**Status**")
    with h6: st.markdown("**Vendor**")
    with h7: st.markdown("** **")
    with h8: st.markdown("** **")
    st.divider()

    for _, row in inv_df.iterrows():
        iid       = row["INVOICEID"]
        tname     = row["TEAMNAME"]
        amt       = float(row["AMOUNT"] or 0.0)
        inv_date  = row["INVOICEDATE"]
        ren_date  = row["RENEWALDATE"]
        status    = row["STATUS"] or "Pending"
        vendor    = row["VENDOR"] or ""

        with st.form(f"row_{iid}", clear_on_submit=False):
            c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(COLS, gap="small")

            with c1:
                idx = team_names.index(tname) if tname in team_names else 0
                e_team = st.selectbox("Team", options=team_names, index=idx, key=f"team_{iid}", label_visibility="hidden")
            with c2:
                e_amt = st.number_input("Amount", min_value=0.0, step=50.0, value=amt, key=f"amt_{iid}", label_visibility="hidden")
            with c3:
                e_inv_date = st.date_input("Invoice Date", value=inv_date, key=f"invdate_{iid}", label_visibility="hidden")
            with c4:
                e_ren_date = st.date_input("Renewal Date", value=ren_date, key=f"rendate_{iid}", label_visibility="hidden")
            with c5:
                e_status = st.selectbox(
                    "Status",
                    ["Paid", "Pending", "Overdue"],
                    index=["Paid", "Pending", "Overdue"].index(status) if status in ["Paid", "Pending", "Overdue"] else 1,
                    key=f"status_{iid}",
                    label_visibility="hidden"
                )
            with c6:
                e_vendor = st.text_input("Vendor", value=vendor, key=f"vendor_{iid}", label_visibility="hidden")
            with c7:
                update_clicked = st.form_submit_button("Update", type="primary", use_container_width=True)
            with c8:
                delete_clicked = st.form_submit_button("Delete", use_container_width=True)

            if update_clicked:
                try:
                    upsert_invoice(
                        invoice_id=iid,
                        application_id=None,
                        team_id=team_name_to_id.get(e_team),
                        invoice_date=e_inv_date,
                        renewal_date=e_ren_date,
                        amount=float(e_amt or 0.0),
                        status=e_status,
                        vendor=e_vendor.strip() or None
                    )
                    invalidate_invoices_cache()
                    st.success("Invoice updated.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to update invoice: {e}")

            if delete_clicked:
                try:
                    delete_invoice(iid)
                    # drop attachment from session if present
                    st.session_state["invoice_attachments"].pop(iid, None)
                    invalidate_invoices_cache()
                    st.success("Invoice deleted.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to delete invoice: {e}")

    with st.expander("View Invoices table (live)"):
        st.dataframe(get_invoices_df(), use_container_width=True)

st.caption(
    "Notes: ‘Cost 2025’ is saved as **INVOICES.AMOUNT**. ‘Cost 2026’ and attachments are not persisted to Snowflake in this version. "
    "For durable files, store in S3/SharePoint and keep a link in the invoice record."
)
