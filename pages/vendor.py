import streamlit as st

st.title("Vendor Management")

st.write("Manage your vendors here. Vendors added here will be available for selection in the Invoice Tracking page.")

# Initialize session state for vendors if not already present
if 'vendors' not in st.session_state:
    st.session_state.vendors = []

st.header("Add New Vendor")
vendor_name = st.text_input("Vendor Name")
add_vendor_button = st.button("Add Vendor")

if add_vendor_button and vendor_name:
    if vendor_name not in st.session_state.vendors:
        st.session_state.vendors.append(vendor_name)
        st.success(f"Vendor \'{vendor_name}\' added.")
    else:
        st.warning(f"Vendor \'{vendor_name}\' already exists.")

st.subheader("Existing Vendors")
if st.session_state.vendors:
    for i, vendor in enumerate(st.session_state.vendors):
        col1, col2 = st.columns([3, 1])
        col1.write(vendor)
        if col2.button(f"Remove {vendor}", key=f"remove_vendor_{i}"):
            st.session_state.vendors.remove(vendor)
            st.experimental_rerun()
else:
    st.info("No vendors defined yet. Add a vendor above.")

# Save vendors to session state for use in other pages
st.session_state["vendors"] = st.session_state.vendors




