import streamlit as st
import uuid
from typing import Optional
from snowflake_db import ensure_tables, fetch_df, upsert_vendor, delete_vendor
try:
    from utils.sidebar import render_global_actions
except Exception:
    def render_global_actions(): pass

st.set_page_config(page_title="Vendors", page_icon="🏷️", layout="wide")
render_global_actions()

if "_tco_init" not in st.session_state:
    ensure_tables()
    st.session_state["_tco_init"] = True

st.title("🏷️ Vendors")

@st.cache_data(show_spinner=False, ttl=180)
def get_vendors_df():
    return fetch_df("SELECT VENDORID, VENDORNAME FROM VENDORS ORDER BY VENDORNAME")

def _vendor_id_for_name_ci(name: str) -> Optional[str]:
    if not name:
        return None
    df = fetch_df("SELECT VENDORID FROM VENDORS WHERE UPPER(VENDORNAME)=UPPER(%s) LIMIT 1", (name.strip(),))
    if df is not None and not df.empty:
        return str(df.iloc[0]["VENDORID"])
    return None

with st.expander("➕ Add / Edit Vendor", expanded=True):
    existing = get_vendors_df()
    options = ["(new)"] + (existing["VENDORNAME"].tolist() if existing is not None and not existing.empty else [])
    choice = st.selectbox("Select Vendor to edit", options)

    if choice == "(new)":
        vendor_id = None
        vendor_name = st.text_input("Vendor Name")
    else:
        row = existing.loc[existing["VENDORNAME"] == choice].iloc[0]
        vendor_id = row["VENDORID"]
        vendor_name = st.text_input("Vendor Name", value=row["VENDORNAME"] or "")

    c1, c2 = st.columns(2)
    if c1.button("💾 Save Vendor"):
        name = (vendor_name or "").strip()
        if not name:
            st.error("Vendor Name is required.")
        else:
            # Uniqueness on VENDORNAME (case-insensitive)
            existing_id = _vendor_id_for_name_ci(name)
            if existing_id and existing_id != (vendor_id or ""):
                st.error(f"A Vendor named '{name}' already exists. Vendor names must be unique.")
            else:
                try:
                    vid = vendor_id or str(uuid.uuid4())
                    upsert_vendor(vid, name)
                    st.cache_data.clear()
                    st.success("Vendor saved.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Save failed: {e}")

    if vendor_id and c2.button("🗑️ Delete Vendor"):
        try:
            delete_vendor(vendor_id)
            st.cache_data.clear()
            st.warning("Vendor deleted.")
            st.rerun()
        except Exception as e:
            st.error(f"Delete failed: {e}")

st.subheader("All Vendors")
st.dataframe(get_vendors_df(), use_container_width=True)
