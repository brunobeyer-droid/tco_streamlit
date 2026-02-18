import streamlit as st
import uuid
from typing import Optional
from core.init import init_page
from db import ensure_tables, fetch_df, upsert_vendor, execute
from utils.toast import toast_error, toast_success

from utils.app_shell import bootstrap_page
bootstrap_page()

page_theme = init_page("Vendors", page_path=__file__)
_page_theme = page_theme
user = st.session_state.get("auth_user") or {}
# --- Back-compat helpers (added non-UI) ---
def delete_vendor(vendor_id: str) -> None:
    """Fallback delete for Vendor; null out references first to avoid FK issues."""
    try:
        execute("UPDATE APPLICATION_GROUPS SET DEFAULT_VENDORID=NULL WHERE DEFAULT_VENDORID=%s", (vendor_id,))
    except Exception:
        pass
    try:
        execute("UPDATE APPLICATIONS SET VENDORID=NULL WHERE VENDORID=%s", (vendor_id,))
    except Exception:
        pass
    execute("DELETE FROM VENDORS WHERE VENDORID=%s", (vendor_id,))

if "_tco_init" not in st.session_state:
    ensure_tables()
    st.session_state["_tco_init"] = True

st.title("Vendors")
st.caption(
    "Maintain mappings, rates, and reference data"
)

@st.cache_data(show_spinner=False, ttl=180)
def get_vendors_df():
    return fetch_df("SELECT VENDORID, VENDORNAME, UPDATED_AT, UPDATED_BY FROM VENDORS ORDER BY VENDORNAME")

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
    if c1.button("Save Vendor", icon=":material/save:"):
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
                    upsert_vendor(vid, name, updated_by=str(user.get("email") or ""))
                    st.cache_data.clear()
                    toast_success("Vendor saved.")
                    st.rerun()
                except Exception as e:
                    toast_error(f"Save failed: {e}")

    if vendor_id and c2.button("Delete Vendor", icon=":material/delete:"):
        try:
            delete_vendor(vendor_id)
            st.cache_data.clear()
            st.warning("Vendor deleted.")
            st.rerun()
        except Exception as e:
            st.error(f"Delete failed: {e}")

st.subheader("All Vendors")
st.dataframe(get_vendors_df(), use_container_width=True)
