import streamlit as st
import uuid
from snowflake_db import ensure_tables as _ensure_tables, fetch_df, upsert_vendor
from utils.sidebar import render_global_actions
render_global_actions()
st.set_page_config(page_title="Vendors", page_icon="🏷️", layout="wide")
st.title("🏷️ Vendors")

# Init once
if not st.session_state.get("_init_vendors_done"):
    _ensure_tables()
    st.session_state["_init_vendors_done"] = True

@st.cache_data(show_spinner=False)
def get_vendors_df():
    return fetch_df("""
        SELECT VENDORID, VENDORNAME
        FROM TCODB.PUBLIC.VENDORS
        ORDER BY VENDORNAME;
    """)

def invalidate_caches():
    get_vendors_df.clear()

# ---------------- Add Vendor (create only) ----------------
st.subheader("Add Vendor")
c1, = st.columns([3])
with c1:
    vendor_name = st.text_input("Vendor Name")

if st.button("Save Vendor", type="primary"):
    if not vendor_name.strip():
        st.warning("Please provide a Vendor Name.")
    else:
        upsert_vendor(vendor_id=str(uuid.uuid4()), vendor_name=vendor_name.strip())
        st.success(f"Vendor '{vendor_name.strip()}' added.")
        invalidate_caches()
        st.rerun()

st.divider()

# ---------------- Filters + List ----------------
st.subheader("Vendors")

df = get_vendors_df()

with st.expander("Filters", expanded=True):
    col1, col2 = st.columns([2, 1])
    with col1:
        query = st.text_input("Vendor name contains", placeholder="e.g. Atlassian, Microsoft...")
    with col2:
        sort_dir = st.selectbox("Sort", ["A → Z", "Z → A"])

fdf = df.copy()
if not fdf.empty and query:
    fdf = fdf[fdf["VENDORNAME"].str.contains(query, case=False, na=False)]

if sort_dir == "A → Z":
    fdf = fdf.sort_values("VENDORNAME", ascending=True)
else:
    fdf = fdf.sort_values("VENDORNAME", ascending=False)

st.metric("Vendors", int(fdf.shape[0]) if not fdf.empty else 0)
st.divider()

if fdf.empty:
    st.info("No vendors found for the selected filters.")
else:
    show = fdf.rename(columns={"VENDORNAME": "Vendor"})[["Vendor"]]
    st.dataframe(show, use_container_width=True)
