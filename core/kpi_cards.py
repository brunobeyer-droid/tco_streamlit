from __future__ import annotations

import streamlit as st


# Color palette (matches Invoice Tracking KPI cards)
KPI_RED = "#E74C3C"
KPI_YELLOW = "#F1C40F"
KPI_ORANGE = "#E67E22"
KPI_GREEN = "#2ECC71"
KPI_GREY = "#95A5A6"


def render_kpi_card(label: str, value: str, color_hex: str) -> None:
    """Render a KPI card matching the Invoices page style."""
    st.markdown(
        f"""
        <div style="
            border-radius:14px;padding:12px 12px;margin:2px 0;
            background:linear-gradient(180deg, {color_hex}1F, transparent);
            border:1px solid {color_hex}33;">
            <div style="font-size:0.78rem;opacity:0.85;">{label}</div>
            <div style="font-size:1.35rem;font-weight:800;">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

