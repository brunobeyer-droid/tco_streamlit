# filename: pages/vendor.py
import streamlit as st
import uuid

# If you need to run this page directly:
# import sys, os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from snowflake_db import ensure_tables, fetch_df, upsert_vendor, delete_vendor


st.set_page_config(page_title="Vendor Management", page_icon="🏷️", layout="wide")
st.title("🏷️ Vendor Management")

# Ensure tables once per session
if not st.session_state.get("_init_vendors_done"):
    ensure_tables()
    st.session_state["_init_vendors_done"] = True

# ---------- Cached readers ----------
@st.cache_data(show_spinner=False)
def get_vendors_df():
    return fetch_df("SELECT VENDORID, VENDORNAME FROM VENDORS ORDER BY UPPER(VENDORNAME);")

def invalidate_vendors_cache():
    get_vendors_df.clear()

# ---------- Add vendor ----------
st.subheader("Add Vendor")
col1, col2 = st.columns([3, 1])
with col1:
    new_vendor = st.text_input("Vendor Name")
with col2:
    if st.button("Save Vendor", type="primary", use_container_width=True):
        name = (new_vendor or "").strip()
        if not name:
            st.warning("Please enter a vendor name.")
        else:
            df = get_vendors_df()
            # prevent duplicates (case-insensitive)
            if not df.empty and name.lower() in (df["VENDORNAME"].str.lower().tolist()):
                st.warning(f"Vendor '{name}' already exists.")
            else:
                vid = str(uuid.uuid4())
                upsert_vendor(vid, name)
                invalidate_vendors_cache()
                st.success(f"Saved vendor '{name}'.")
                st.rerun()

st.divider()

# ---------- Current vendors ----------
st.subheader("Current Vendors")
vdf = get_vendors_df()
if vdf.empty:
    st.info("No vendors yet. Add one above.")
else:
    # Header
    COLS = [3, 1, 1]  # Name | Update | Delete
    h1, h2, h3 = st.columns(COLS, gap="small")
    with h1: st.markdown("**Vendor Name**")
    with h2: st.markdown("** **")
    with h3: st.markdown("** **")
    st.divider()

    for _, row in vdf.iterrows():
        vid = row["VENDORID"]
        vname = row["VENDORNAME"]

        with st.form(f"row_{vid}", clear_on_submit=False):
            c1, c2, c3 = st.columns(COLS, gap="small")
            with c1:
                edited_name = st.text_input("Vendor Name", value=vname, key=f"name_{vid}", label_visibility="hidden")
            with c2:
                update_clicked = st.form_submit_button("Update", type="primary", use_container_width=True)
            with c3:
                delete_clicked = st.form_submit_button("Delete", use_container_width=True)

            if update_clicked:
                nm = (edited_name or "").strip()
                if not nm:
                    st.warning("Vendor name cannot be blank.")
                else:
                    upsert_vendor(vid, nm)  # same ID, new name
                    invalidate_vendors_cache()
                    st.success(f"Updated vendor to '{nm}'.")
                    st.rerun()

            if delete_clicked:
                delete_vendor(vid)
                invalidate_vendors_cache()
                st.success(f"Deleted vendor '{vname}'.")
                st.rerun()

    with st.expander("View Vendors table (live)"):
        st.dataframe(get_vendors_df(), use_container_width=True)

st.caption("These vendors are stored in Snowflake (TCODB.PUBLIC.VENDORS) and will be available for selection on the Invoices page.")
