
import streamlit as st

st.title("Team Composition")

st.write("Define your teams, link them to programs, and specify their workforce composition here.")

# Retrieve programs from session state
programs = st.session_state.get("programs", [])

if not programs:
    st.warning("Please define programs in the \'Programs and Workforce\' page first.")
else:
    # Initialize session state for teams if not already present
    if 'teams_data' not in st.session_state:
        st.session_state.teams_data = {}

    st.header("Define Teams per Program")

    selected_program = st.selectbox("Select a Program to add/edit teams", programs)

    if selected_program:
        if selected_program not in st.session_state.teams_data:
            st.session_state.teams_data[selected_program] = []

        st.subheader(f"Teams for {selected_program}")

        with st.form(key=f"add_team_form_{selected_program}"):
            new_team_name = st.text_input(f"New Team Name for {selected_program}")
            is_msp = st.checkbox(f"Is {new_team_name} an MSP (Managed Service Provider) team?", key=f"is_msp_{selected_program}_{new_team_name}")
            st.markdown("**Workforce Composition (FTEs) for this Team:**")
            team_workforce = {
                'Engineers': st.number_input('Engineers (FTEs)', min_value=0.0, value=0.0, key=f"eng_{selected_program}_{new_team_name}"),
                'Product Managers': st.number_input('Product Managers (FTEs)', min_value=0.0, value=0.0, key=f"pm_{selected_program}_{new_team_name}"),
                'Designers': st.number_input('Designers (FTEs)', min_value=0.0, value=0.0, key=f"des_{selected_program}_{new_team_name}"),
                'QA Engineers': st.number_input('QA Engineers (FTEs)', min_value=0.0, value=0.0, key=f"qa_{selected_program}_{new_team_name}"),
                'DevOps Engineers': st.number_input('DevOps Engineers (FTEs)', min_value=0.0, value=0.0, key=f"devops_{selected_program}_{new_team_name}")
            }
            add_team_button = st.form_submit_button(f"Add Team to {selected_program}")

            if add_team_button and new_team_name:
                # Check if team already exists
                team_exists = False
                for team_entry in st.session_state.teams_data[selected_program]:
                    if team_entry["name"] == new_team_name:
                        team_exists = True
                        break

                if not team_exists:
                    st.session_state.teams_data[selected_program].append({"name": new_team_name, "workforce": team_workforce, "is_msp": is_msp})
                    st.success(f"Team \'{new_team_name}\' added to {selected_program}.")
                else:
                    st.warning(f"Team \'{new_team_name}\' already exists in {selected_program}.")

        st.markdown("--- ")
        st.subheader(f"Existing Teams in {selected_program}")
        if st.session_state.teams_data[selected_program]:
            for i, team_entry in enumerate(st.session_state.teams_data[selected_program]):
                team_name = team_entry["name"]
                team_workforce = team_entry["workforce"]
                team_is_msp = team_entry["is_msp"]
                
                st.write(f"**Team: {team_name}** (MSP: {team_is_msp})")
                
                # Allow editing of team name, MSP flag, and workforce
                with st.expander(f"Edit {team_name}"):
                    edited_team_name = st.text_input("Team Name", value=team_name, key=f"edit_team_name_{selected_program}_{i}")
                    edited_is_msp = st.checkbox("Is MSP?", value=team_is_msp, key=f"edit_is_msp_{selected_program}_{i}")
                    
                    st.markdown("**Edit Workforce Composition (FTEs):**")
                    edited_workforce = {}
                    for role, fte_value in team_workforce.items():
                        edited_workforce[role] = st.number_input(
                            f"{role} (FTEs)",
                            min_value=0.0,
                            value=float(fte_value),
                            key=f"edit_fte_{selected_program}_{i}_{role}"
                        )
                    
                    # Update logic
                    if edited_team_name != team_name or edited_is_msp != team_is_msp or edited_workforce != team_workforce:
                        st.session_state.teams_data[selected_program][i]["name"] = edited_team_name
                        st.session_state.teams_data[selected_program][i]["is_msp"] = edited_is_msp
                        st.session_state.teams_data[selected_program][i]["workforce"] = edited_workforce
                        st.success(f"Team ‘{team_name}’ updated.")
                        st.experimental_rerun()

                if st.button(f"Remove {team_name}", key=f"remove_team_{selected_program}_{i}"):
                    st.session_state.teams_data[selected_program].pop(i)
                    st.experimental_rerun()
        else:
            st.info(f"No teams defined for {selected_program} yet. Add a team above.")

    # Save teams data to session state for use in other pages
    st.session_state["teams_data"] = st.session_state.teams_data


