
import streamlit as st
import pandas as pd
from collections import defaultdict

st.set_page_config(page_title="Cost Summary", page_icon="📊", layout="wide")
st.title("📊 Cost Summary")

# --------------------------
# Read shared state
# --------------------------
programs = st.session_state.get("programs", [])
teams_data = st.session_state.get("teams_data", {})  # { program: [ { "team_name": str, "workforce": number }, ... ] }
invoices = st.session_state.get("invoices", [])      # [ { "program": str?, "team": str?, "amount": number?, ... }, ... ]
program_fte_map = st.session_state.get("program_fte", {})  # { program_name: fte } (if your programs.py stores it)

# --------------------------
# Helpers
# --------------------------
def safe_number(x, default=0.0):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        return float(str(x).strip())
    except Exception:
        return default

def pick_amount(inv: dict):
    for k in ("amount", "invoice_amount", "total_cost", "value"):
        if k in inv:
            return safe_number(inv[k], 0.0)
    return 0.0

def pick_program(inv: dict):
    for k in ("program", "selected_program", "program_name"):
        if k in inv and inv[k]:
            return str(inv[k])
    return None

def pick_team(inv: dict):
    for k in ("team", "team_name", "selected_team"):
        if k in inv and inv[k]:
            return str(inv[k])
    return None

# --------------------------
# Build summaries
# --------------------------

# 1) Program FTE (direct, if provided)
program_fte_rows = []
for p in programs:
    fte = safe_number(program_fte_map.get(p, 0))
    program_fte_rows.append({"Program": p, "Program FTE (declared)": fte})
df_program_fte = pd.DataFrame(program_fte_rows) if program_fte_rows else pd.DataFrame(columns=["Program", "Program FTE (declared)"])

# 2) Team FTE totals per program (sum of team 'workforce')
team_fte_rows = []
for p, teams in (teams_data or {}).items():
    total_team_fte = 0.0
    for t in teams or []:
        total_team_fte += safe_number(t.get("workforce", 0))
    team_fte_rows.append({"Program": p, "Total Team FTE": total_team_fte})
df_team_fte = pd.DataFrame(team_fte_rows) if team_fte_rows else pd.DataFrame(columns=["Program", "Total Team FTE"])

# 3) Invoices totals by Program and by Team
by_program = defaultdict(float)
by_team = defaultdict(float)

for inv in invoices or []:
    amt = pick_amount(inv)
    prog = pick_program(inv) or "Unspecified Program"
    team = pick_team(inv) or "Unspecified Team"
    by_program[prog] += amt
    by_team[(prog, team)] += amt

# Build DataFrames safely (avoid KeyError when empty)
df_inv_prog = pd.DataFrame([{"Program": k, "Invoice Total": v} for k, v in by_program.items()])
if not df_inv_prog.empty and "Invoice Total" in df_inv_prog.columns:
    df_inv_prog = df_inv_prog.sort_values("Invoice Total", ascending=False)
else:
    df_inv_prog = pd.DataFrame(columns=["Program", "Invoice Total"])

df_inv_team = pd.DataFrame([{"Program": k[0], "Team": k[1], "Invoice Total": v} for k, v in by_team.items()])
if not df_inv_team.empty and "Invoice Total" in df_inv_team.columns:
    df_inv_team = df_inv_team.sort_values(["Program", "Invoice Total"], ascending=[True, False])
else:
    df_inv_team = pd.DataFrame(columns=["Program", "Team", "Invoice Total"])

# --------------------------
# Display
# --------------------------
c1, c2, c3 = st.columns(3)
with c1:
    st.metric("Programs", len(programs))
with c2:
    team_count = sum(len(v or []) for v in teams_data.values()) if teams_data else 0
    st.metric("Teams", team_count)
with c3:
    total_invoices = float(df_inv_prog["Invoice Total"].sum()) if not df_inv_prog.empty else 0.0
    st.metric("Total Invoices", f"{total_invoices:,.2f}")

st.divider()

left, right = st.columns(2, gap="large")

with left:
    st.subheader("Program FTE Summary")
    if not df_program_fte.empty:
        st.dataframe(df_program_fte, use_container_width=True)
        # Simple bar
        try:
            st.bar_chart(df_program_fte.set_index("Program")["Program FTE (declared)"])
        except Exception:
            pass
    else:
        st.info("No Program FTE data found. If your programs.py stores FTE into st.session_state['program_fte'], it will appear here.")

with right:
    st.subheader("Team FTE by Program")
    if not df_team_fte.empty:
        st.dataframe(df_team_fte, use_container_width=True)
        # Bar of team totals
        try:
            st.bar_chart(df_team_fte.set_index("Program")["Total Team FTE"])
        except Exception:
            pass
    else:
        st.info("No Team FTE data found. Make sure teams.py stores teams in st.session_state['teams_data'] with a 'workforce' field.")

st.divider()

st.subheader("Invoice Totals")
colA, colB = st.columns(2, gap="large")
with colA:
    st.markdown("**By Program**")
    if not df_inv_prog.empty:
        st.dataframe(df_inv_prog, use_container_width=True)
        try:
            st.bar_chart(df_inv_prog.set_index("Program")["Invoice Total"])
        except Exception:
            pass
    else:
        st.info("No invoices found. Ensure invoice_tracking.py stores a list in st.session_state['invoices'] with an 'amount' (or 'invoice_amount'/'total_cost') and 'program'.")

with colB:
    st.markdown("**By Program & Team**")
    if not df_inv_team.empty:
        st.dataframe(df_inv_team, use_container_width=True)
    else:
        st.info("No team-level invoice breakdown found. Make sure invoices include 'program' and 'team'.")
