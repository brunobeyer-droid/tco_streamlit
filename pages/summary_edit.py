
import streamlit as st

st.title("Summary and Edit Data")

st.write("Here you can view a summary of your programs, teams, and cost details. Click on the links below to navigate to the respective pages for editing.")

# Retrieve data from session state
programs = st.session_state.get("programs", [])
teams_data = st.session_state.get("teams_data", {})

st.header("Programs Overview")
if programs:
    for program in programs:
        st.markdown(f"- **{program}**")
else:
    st.info("No programs defined yet. Go to the Programs and Workforce page to add programs.")

st.markdown("--- ")
st.header("Teams Overview")
if teams_data:
    for program, teams in teams_data.items():
        st.markdown(f"### Program: {program}")
        if teams:
            for team_entry in teams:
                team_name = team_entry["name"]
                team_is_msp = team_entry.get("is_msp", False)
                st.markdown(f"- **{team_name}** (MSP: {team_is_msp})")
                st.json(team_entry["workforce"])
        else:
            st.info(f"No teams defined for program \'{program}\'.")
else:
    st.info("No teams defined yet. Go to the Team Composition page to add teams.")

st.markdown("--- ")
st.header("Cost Details Overview")
st.write("Cost details are managed on the \"Cost Details & TCO Calculation\" page.")

st.markdown("--- ")
st.header("Quick Links to Edit Pages")
st.markdown("**Programs and Workforce:** [Go to Programs and Workforce page](Programs_and_Workforce)")
st.markdown("**Team Composition:** [Go to Team Composition page](Team_Composition)")
st.markdown("**Cost Details & TCO Calculation:** [Go to Cost Details & TCO Calculation page](Cost_Details___TCO_Calculation)")


