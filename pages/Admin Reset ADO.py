# pages/Admin – Reset ADO.py
import streamlit as st
import pandas as pd

from snowflake_db import (
    reset_ado_calc_artifacts,
    ensure_ado_minimal_tables,
    list_views,
    drop_view,
    drop_views_by_prefix,
)

st.set_page_config(page_title="Admin – Reset ADO & Views", layout="wide")
st.title("🧹 Admin – Reset ADO & Clean Views")

tab_schema, tab_views = st.tabs(["ADO Reset", "Views Cleaner"])

# -------------------------
# Tab 1: ADO Reset
# -------------------------
with tab_schema:
    st.warning(
        "This will remove **calculation artifacts** created earlier:\n"
        "• Drop tables: `ADO_FEATURE_COST_ESTIMATE`, `EFFORT_SPLIT_RULES`\n"
        "• Remove calc columns from `ADO_FEATURES` (e.g., `ADO_YEAR`, any `EFF_*`, `COST_*`, `FTEPI_*`, `EST_*`)\n\n"
        "It **keeps** raw `ADO_FEATURES` and mapping tables by default."
    )

    drop_maps = st.checkbox(
        "Also drop mapping tables (`MAP_ADO_TEAM_TO_TCO_TEAM`, `MAP_ADO_APP_TO_TCO_GROUP`)?",
        value=False,
    )

    if st.button("🚨 Run ADO Reset Now", type="primary"):
        with st.spinner("Resetting ADO schema..."):
            reset_ado_calc_artifacts(drop_mappings=drop_maps)
            ensure_ado_minimal_tables()
        st.success("Done. ADO is now back to the minimal schema (raw features + mappings).")

# -------------------------
# Tab 2: Views Cleaner
# -------------------------
with tab_views:
    st.subheader("Drop calculation views (V_*)")

    st.caption(
        "Below are all views in the current Snowflake database/schema. "
        "Views starting with **V_** are preselected. Uncheck anything you want to keep."
    )

    # Load all views and mark default selection for those starting with V_
    df_views = list_views()
    if df_views.empty:
        st.info("No views found in the current schema.")
    else:
        df = df_views.copy()
        df["SELECT"] = df["VIEW_NAME"].apply(lambda x: str(x).upper().startswith("V_"))

        st.dataframe(
            df[["VIEW_NAME", "SCHEMA_NAME", "DATABASE_NAME", "SELECT"]],
            use_container_width=True,
            height=420,
        )

        col1, col2 = st.columns([1, 1])
        if col1.button("Select all V_*"):
            df["SELECT"] = df["VIEW_NAME"].str.upper().str.startswith("V_")
        if col2.button("Clear all"):
            df["SELECT"] = False

        # Multi-select list for safety confirmation
        selected = df.loc[df["SELECT"] == True, "VIEW_NAME"].tolist()
        st.write(f"Selected to drop: {len(selected)} view(s)")

        if st.button("🗑️ Drop selected views", type="primary", disabled=len(selected) == 0):
            dropped = []
            with st.spinner("Dropping selected views..."):
                for v in selected:
                    try:
                        drop_view(v)
                        dropped.append(v)
                    except Exception as e:
                        st.error(f"Failed to drop {v}: {e}")
            if dropped:
                st.success(f"Dropped {len(dropped)} view(s): {', '.join(dropped)}")

        st.markdown("---")
        st.markdown("**Quick action**")
        if st.button("🧨 Drop ALL views starting with `V_`"):
            with st.spinner("Dropping all views with prefix V_ ..."):
                dropped = drop_views_by_prefix("V_")
            if dropped:
                st.success(f"Dropped {len(dropped)} view(s): {', '.join(dropped)}")
            else:
                st.info("No `V_` views found (or already dropped).")
