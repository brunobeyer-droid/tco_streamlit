
import streamlit as st

st.title("Detailed TCO Summary")

st.write("This page provides a detailed summary of the Total Cost of Ownership calculation, including breakdowns by programs, teams, and cost categories.")

# Retrieve data from session state
programs = st.session_state.get("programs", [])
teams_data = st.session_state.get("teams_data", {})

# Retrieve cost details from session state (assuming they are set on the cost_details page)
initial_development_cost = st.session_state.get("initial_dev_cost", 0.0)
hardware_acquisition_cost = st.session_state.get("hardware_acq_cost", 0.0)
setup_integration_cost = st.session_state.get("setup_integration_cost", 0.0)

maintenance_support_cost = st.session_state.get("maintenance_cost", 0.0)
infrastructure_hosting_cost = st.session_state.get("infra_cost", 0.0)

technical_debt_remediation = st.session_state.get("tech_debt_cost", 0.0)
security_compliance_cost = st.session_state.get("security_cost", 0.0)
training_cost = st.session_state.get("training_cost", 0.0)

num_years = st.session_state.get("num_years_slider", 3)

# Calculate totals (re-calculate to ensure consistency)
initial_costs_total = initial_development_cost + hardware_acquisition_cost + setup_integration_cost

total_license_cost = 0.0
if programs:
    for program in programs:
        # Assuming license_cost is stored per program in session state from cost_details.py
        total_license_cost += st.session_state.get(f"license_cost_{program}", 0.0)

recurring_costs_total_excluding_workforce = maintenance_support_cost + infrastructure_hosting_cost + total_license_cost

other_costs_total = technical_debt_remediation + security_compliance_cost + training_cost

# Calculate total workforce FTEs
total_workforce_fte_sum = 0.0
for program in programs:
    if program in teams_data and teams_data[program]:
        for team_entry in teams_data[program]:
            team_workforce = team_entry["workforce"]
            for role, fte in team_workforce.items():
                total_workforce_fte_sum += fte

# TCO calculation (excluding workforce cost, as per previous instruction)
total_tco = initial_costs_total + (recurring_costs_total_excluding_workforce + other_costs_total) * num_years

st.header("Overall TCO Calculation")
st.markdown(f"## Estimated Total Cost of Ownership over {num_years} years: ${total_tco:,.2f}")
st.write("*(Note: Workforce costs are captured as FTEs and are intended for calculation in PowerBI, hence excluded from this TCO sum.)*")

st.subheader("Cost Breakdown")
st.markdown(f"- **Total Initial Costs:** ${initial_costs_total:,.2f}")
st.markdown(f"- **Total Annual Recurring Costs (excluding workforce):** ${recurring_costs_total_excluding_workforce:,.2f}")
st.markdown(f"- **Total Annual Other Costs:** ${other_costs_total:,.2f}")

st.header("Programs and Teams Overview")
if programs:
    for program in programs:
        st.markdown(f"### Program: {program}")
        if program in teams_data and teams_data[program]:
            for team_entry in teams_data[program]:
                team_name = team_entry["name"]
                team_is_msp = team_entry.get("is_msp", False)
                st.markdown(f"#### Team: {team_name} (MSP: {team_is_msp})")
                st.write("**Workforce FTEs:**")
                for role, fte in team_entry["workforce"].items():
                    st.write(f"- {role}: {fte} FTEs")
        else:
            st.info(f"No teams defined for program \'{program}\'.")
else:
    st.info("No programs defined. Please define programs and teams in the respective pages.")

st.subheader(f"Total Workforce FTEs Across All Teams: {total_workforce_fte_sum:,.2f}")

st.markdown("--- ")
st.write("**Note:** This summary reflects the data entered in the other pages. Ensure all data is up-to-date for accurate TCO calculation.")


