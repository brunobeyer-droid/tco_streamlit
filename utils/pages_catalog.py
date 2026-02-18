from __future__ import annotations

from typing import Dict, List, Tuple, Any

import pandas as pd
import streamlit as st

ROLE_LEVELS = {"VIEWER": 1, "CONTRIBUTOR": 2, "ADMIN": 3}

PAGES_CATALOG: List[Tuple[str, str, str, str, str, str]] = [
    ("Core", "pages/0_Welcome.py", "Welcome", ":material/home:", "welcome", "VIEWER"),
    ("Core", "pages/1_Dashboard.py", "Dashboard", ":material/dashboard:", "dashboard", "VIEWER"),
    ("Core", "pages/1_Insights.py", "Insights", ":material/insights:", "insights", "VIEWER"),
    ("Core", "pages/2_Roadmap.py", "Roadmap", ":material/timeline:", "roadmap", "VIEWER"),
    ("Finance", "pages/2_Invoices.py", "Invoices", ":material/receipt_long:", "invoices", "CONTRIBUTOR"),
    ("Finance", "pages/2_Contracts.py", "Contracts", ":material/description:", "contracts", "CONTRIBUTOR"),
    ("Finance", "pages/7_Rates.py", "Rates", ":material/paid:", "rates", "CONTRIBUTOR"),
    ("Finance", "pages/Budget.py", "Budget", ":material/assessment:", "budget", "CONTRIBUTOR"),
    ("Master Data", "pages/3_Programs.py", "Programs", ":material/account_tree:", "programs", "CONTRIBUTOR"),
    ("Master Data", "pages/4_Teams.py", "Teams", ":material/groups:", "teams", "CONTRIBUTOR"),
    ("Master Data", "pages/5_Vendors.py", "Vendors", ":material/store:", "vendors", "CONTRIBUTOR"),
    ("Master Data", "pages/6_Applications.py", "Applications", ":material/apps:", "applications", "CONTRIBUTOR"),
    ("Help", "pages/9_How_To.py", "How To", ":material/help:", "how-to", "VIEWER"),
    ("Admin", "pages/10_Settings.py", "Settings", ":material/settings:", "settings", "ADMIN"),
    ("Admin", "pages/11_Data_Quality.py", "Data Quality", ":material/rule_settings:", "data-quality", "CONTRIBUTOR"),
    ("Admin", "pages/99_Admin.py", "Admin", ":material/admin_panel_settings:", "admin", "ADMIN"),
]


def can_access(user_role: str, min_role: str) -> bool:
    return ROLE_LEVELS.get(str(user_role or "").upper(), 0) >= ROLE_LEVELS.get(str(min_role or "").upper(), 0)


def build_navigation(user_role: str) -> Tuple[Dict[str, List[st.Page]], Dict[str, List[Dict[str, Any]]]]:
    is_control_global_admin = bool(st.session_state.get("_is_control_global_admin", False))
    # Special case: allow a minimal Admin-only nav when the user is authenticated
    # but has no portfolio access. This enables onboarding the first portfolio.
    if (
        st.session_state.get("_app_block_reason") == "NO_PORTFOLIO_ACCESS"
        and can_access(user_role, "ADMIN")
        and is_control_global_admin
    ):
        pages_map = {
            "Admin": [
                st.Page("pages/99_Admin.py", title="Portfolios", icon=":material/folder:", url_path="admin-portfolios"),
                st.Page("pages/99_Admin.py", title="Control DB", icon=":material/storage:", url_path="admin-control-db"),
                st.Page("pages/10_Settings.py", title="Settings", icon=":material/settings:", url_path="settings"),
            ]
        }
        pages_meta = {
            "Admin": [
                {"title": "Portfolios", "path": "pages/99_Admin.py", "icon": ":material/folder:", "url_path": "admin-portfolios"},
                {"title": "Control DB", "path": "pages/99_Admin.py", "icon": ":material/storage:", "url_path": "admin-control-db"},
                {"title": "Settings", "path": "pages/10_Settings.py", "icon": ":material/settings:", "url_path": "settings"},
            ]
        }
        return pages_map, pages_meta

    pages_map: Dict[str, List[st.Page]] = {}
    pages_meta: Dict[str, List[Dict[str, Any]]] = {}
    for section, path, title, icon, url_path, min_role in PAGES_CATALOG:
        # Control DB/global admin only page. Portfolio-level admins must not see this page.
        if path == "pages/99_Admin.py" and not is_control_global_admin:
            continue
        if can_access(user_role, min_role):
            page = st.Page(path, title=title, icon=icon, url_path=url_path)
            pages_map.setdefault(section, []).append(page)
            pages_meta.setdefault(section, []).append(
                {"title": title, "path": path, "icon": icon, "url_path": url_path}
            )
    if pages_map:
        return pages_map, pages_meta
    # Fallback: minimal navigation to keep the app usable
    fallback: Dict[str, List[st.Page]] = {
        "Core": [
            st.Page("pages/0_Welcome.py", title="Welcome", icon=":material/home:", url_path="welcome"),
            st.Page("pages/1_Dashboard.py", title="Dashboard", icon=":material/dashboard:", url_path="dashboard"),
            st.Page("pages/1_Insights.py", title="Insights", icon=":material/insights:", url_path="insights"),
        ],
        "Planning": [
            st.Page("pages/2_Roadmap.py", title="Roadmap", icon=":material/timeline:", url_path="roadmap"),
        ],
        "Settings": [
            st.Page("pages/10_Settings.py", title="Settings", icon=":material/settings:", url_path="settings"),
        ],
    }
    fallback_meta: Dict[str, List[Dict[str, Any]]] = {
        "Core": [
            {"title": "Welcome", "path": "pages/0_Welcome.py", "icon": ":material/home:", "url_path": "welcome"},
            {"title": "Dashboard", "path": "pages/1_Dashboard.py", "icon": ":material/dashboard:", "url_path": "dashboard"},
            {"title": "Insights", "path": "pages/1_Insights.py", "icon": ":material/insights:", "url_path": "insights"},
        ],
        "Planning": [
            {"title": "Roadmap", "path": "pages/2_Roadmap.py", "icon": ":material/timeline:", "url_path": "roadmap"},
        ],
        "Settings": [
            {"title": "Settings", "path": "pages/10_Settings.py", "icon": ":material/settings:", "url_path": "settings"},
        ],
    }
    if can_access(user_role, "ADMIN") and is_control_global_admin:
        fallback.setdefault("Admin", []).append(
            st.Page("pages/99_Admin.py", title="Admin", icon=":material/admin_panel_settings:", url_path="admin")
        )
        fallback_meta.setdefault("Admin", []).append(
            {"title": "Admin", "path": "pages/99_Admin.py", "icon": ":material/admin_panel_settings:", "url_path": "admin"}
        )
    return fallback, fallback_meta


def permissions_matrix_df() -> pd.DataFrame:
    rows = []
    for section, path, title, icon, url_path, min_role in PAGES_CATALOG:
        rows.append(
            {
                "Section": section,
                "Title": title,
                "Path": path,
                "URL Path": url_path,
                "Min Role": min_role,
                "Viewer": can_access("VIEWER", min_role),
                "Contributor": can_access("CONTRIBUTOR", min_role),
                "Admin": can_access("ADMIN", min_role),
            }
        )
    return pd.DataFrame(rows)
