import streamlit as st
import datetime

st.title("Invoice Tracking")

st.write("Track license invoices, link them to teams, and manage vendor details.")

# Retrieve vendors and teams data from session state
vendors = st.session_state.get("vendors", [])
programs = st.session_state.get("programs", [])
teams_data = st.session_state.get("teams_data", {})

# Initialize session state for invoices if not already present
if 'invoices' not in st.session_state:
    st.session_state.invoices = []

if not programs:
    st.warning("Please define programs and teams in the respective pages first.")
else:
    st.header("Add New License Invoice")

    with st.form(key="add_invoice_form"):
        # Team selection
        selected_program_for_invoice = st.selectbox("Select Program", programs, key="invoice_program_select")
        
        teams_in_selected_program = []
        if selected_program_for_invoice and selected_program_for_invoice in teams_data:
            teams_in_selected_program = [team['name'] for team in teams_data[selected_program_for_invoice]]

        selected_team_for_invoice = st.selectbox("Select Team (can be linked to multiple licenses)", teams_in_selected_program, key="invoice_team_select")

        # Vendor lookup
        vendor_name = st.selectbox("Vendor Name", vendors, key="invoice_vendor_name")
        if not vendors:
            st.warning("No vendors defined. Please go to the Vendor Management page to add vendors.")

        product_name = st.text_input("Product Name", key="invoice_product_name")
        renewal_date = st.date_input("Renewal Date", datetime.date.today(), key="invoice_renewal_date")
        cost_2025 = st.number_input("Cost 2025 ($)", min_value=0.0, value=0.0, key="invoice_cost_2025")
        cost_2026 = st.number_input("Cost 2026 ($)", min_value=0.0, value=0.0, key="invoice_cost_2026")
        details = st.text_area("Details (log of notes)", key="invoice_details")
        invoice_completed = st.checkbox("Invoice Completed", key="invoice_completed_flag")
        
        # File attachment (Note: For persistent storage, you'd need a backend)
        uploaded_file = st.file_uploader("Attach File (e.g., invoice PDF)", type=["pdf", "png", "jpg", "jpeg"], key="invoice_file_attachment")

        add_invoice_button = st.form_submit_button("Add Invoice")

        if add_invoice_button:
            if not vendor_name:
                st.error("Please select a Vendor Name.")
            elif not product_name:
                st.error("Please enter a Product Name.")
            elif not selected_team_for_invoice:
                st.error("Please select a Team.")
            else:
                new_invoice = {
                    "program": selected_program_for_invoice,
                    "team": selected_team_for_invoice,
                    "vendor_name": vendor_name,
                    "product_name": product_name,
                    "renewal_date": renewal_date.strftime("%Y-%m-%d"),
                    "cost_2025": cost_2025,
                    "cost_2026": cost_2026,
                    "details": details,
                    "invoice_completed": invoice_completed,
                    "file_attachment_name": uploaded_file.name if uploaded_file else None,
                    "file_attachment_data": uploaded_file.getvalue().decode("latin-1") if uploaded_file else None # Storing as string for simplicity, not ideal for large files
                }
                st.session_state.invoices.append(new_invoice)
                st.success(f"Invoice for {product_name} added successfully!")

    st.markdown("--- ")
    st.header("Existing Invoices")
    if st.session_state.invoices:
        for i, invoice in enumerate(st.session_state.invoices):
            st.subheader(f"Invoice for {invoice['product_name']} (Team: {invoice['team']})")
            st.write(f"**Vendor:** {invoice['vendor_name']}")
            st.write(f"**Renewal Date:** {invoice['renewal_date']}")
            st.write(f"**Cost 2025:** ${invoice['cost_2025']:,.2f}")
            st.write(f"**Cost 2026:** ${invoice['cost_2026']:,.2f}")
            st.write(f"**Invoice Completed:** {invoice['invoice_completed']}")
            st.write(f"**Details:** {invoice['details']}")
            if invoice['file_attachment_name']:
                st.write(f"**Attached File:** {invoice['file_attachment_name']}")
                # You could add a download button here if you want to allow downloading
                # st.download_button(label="Download File", data=invoice['file_attachment_data'].encode("latin-1"), file_name=invoice['file_attachment_name'])

            if st.button(f"Remove Invoice for {invoice['product_name']}", key=f"remove_invoice_{i}"):
                st.session_state.invoices.pop(i)
                st.experimental_rerun()
    else:
        st.info("No invoices recorded yet.")

# Save invoices to session state
st.session_state["invoices"] = st.session_state.invoices