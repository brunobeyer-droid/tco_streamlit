# Summary: restructure How-To tabs/content for NEXT onboarding flows, remove page links, and clarify exec-friendly guidance.
# Changes: new NEXT Onboarding tab, consolidated scenario definitions in Core Concepts, and simplified Page-by-Page Guide (no links).
# pages/9_How_To.py — User guide
from __future__ import annotations

import datetime
import streamlit as st
from core.branding import ACRONYM, APP_NAME
from core.init import init_page
from utils.theme import THEME_PALETTES
from utils.completeness_config import load_completeness_config

from utils.app_shell import bootstrap_page
bootstrap_page()

page_theme = init_page("How To", page_path=__file__)
_page_theme = page_theme
_palette = THEME_PALETTES.get(_page_theme, THEME_PALETTES["light"])
user = st.session_state.get("auth_user") or {}

st.title(f"{APP_NAME} — How It Works & How to Use It")
st.caption(
    "A practical guide to scenarios, data sources, mappings, onboarding, and troubleshooting."
)
st.caption(f"Last updated: {datetime.date.today().isoformat()}")

TAB_SPECS = [
    ("getting_started", "Getting Started"),
    ("next_onboarding", "NEXT Onboarding"),
    ("core_concepts", "Core Concepts"),
    ("common_tasks", "Common Tasks"),
    ("data_quality", "Data Quality & Troubleshooting"),
    ("faq", "FAQ"),
    ("page_guide", "Page-by-Page Guide"),
]
TAB_LABELS = {k: v for k, v in TAB_SPECS}
TAB_LABEL_TO_KEY = {v: k for k, v in TAB_SPECS}
DEFAULT_TAB = "getting_started"

if "howto_focus_id" not in st.session_state:
    st.session_state["howto_focus_id"] = ""
if "howto_selected_tab" not in st.session_state:
    st.session_state["howto_selected_tab"] = DEFAULT_TAB
if "howto_selected_tab_label" not in st.session_state:
    st.session_state["howto_selected_tab_label"] = TAB_LABELS.get(st.session_state["howto_selected_tab"], TAB_SPECS[0][1])
if "howto_nav" in st.session_state:
    nav = st.session_state.pop("howto_nav") or {}
    st.session_state["howto_selected_tab"] = nav.get("tab", DEFAULT_TAB)
    st.session_state["howto_focus_id"] = nav.get("focus_id", "")
    st.session_state["howto_selected_tab_label"] = TAB_LABELS.get(st.session_state["howto_selected_tab"], TAB_SPECS[0][1])

_cfg = load_completeness_config()
_thr_cfg = _cfg.get("thresholds", {}) or {}
_high_thr = _thr_cfg.get("high", 90)
_med_thr = _thr_cfg.get("med", 70)

what_is_next = f"""**{APP_NAME} stands for {ACRONYM}.**

Intended for Portfolio Managers, Program Managers, Product Owners, and SDMs.

NEXT helps compare **Baseline**, **Expected**, and **Actual** costs across programs, teams, and application groups to support planning and governance.
"""

interpretation_disclaimer = """NEXT is an analytical and planning tool to estimate **Baseline**, **Expected**, and **Actual** costs across programs, teams, and application groups.

Cost views come from Azure DevOps effort allocation, Apptio actuals (where available), and manual user inputs (rates, mappings, headcount). Variances may occur due to incomplete mappings, program-level actuals, timing differences, or missing updates. Outputs are for directional insights and scenario evaluation, not an authoritative financial statement.
"""

po_role = """Product Owners are the main stewards responsible for keeping data and mappings current. They are accountable to:
- Provide high-level IT work estimate for work items.
- Define and steward 0–1 Year Execution Plan (PI Roadmap).
- Monitor solutions usage and assist with license renewals.
- Understand and steward ongoing product cost.
"""

po_support = """NEXT aims to automate and assist Product Owners over time by reducing manual reconciliation, highlighting gaps, and providing early signals.
"""

getting_started_checklist = """1. Onboard and map applications.
2. Validate program and team structure.
3. Maintain execution signals in Azure DevOps.
4. Configure and review rates and headcount assumptions.
5. Review actuals and coverage (Apptio NWF actuals and invoices).
6. Monitor data quality and variances using the Data Quality page.
"""

onboarding_sequence = """Recommended onboarding sequence: **Programs → Teams → Applications**.

Why the sequence matters: you establish structure first, then teams under programs, then map applications to teams for accurate attribution.
"""

onboarding_prereqs = """- ADO features/signals must be loaded for onboarding candidates to appear; otherwise tables show no ADO data.
- **Include global (outside scope)**: enable when you need to view or map candidates outside the current scope.
"""

onboard_programs = """**Where**: Programs page → **Onboard** section.

Steps:
1. Open **Programs** and expand **Onboard**.
2. Select an ADO program candidate (self-onboard when available).
3. Create the program entity and map it.
4. Confirm it appears in program filters and coverage improves.
"""

onboard_teams = """**Where**: Teams page → **Onboard** section.

Steps:
1. Filter by Program first.
2. Select unmapped ADO team candidates.
3. Create or match the team, then map it.
4. Confirm the team shows under the Program.
5. If you use program overhead teams, keep naming consistent.
"""

onboard_apps = """**Where**: Applications page → **Onboard** section (unmapped ADO app names).

Steps:
1. Review the list of unmapped ADO app names.
2. Create or map to a NEXT Application Group.
3. **Unassigned** means unmapped/out-of-scope work, not a real app group.
4. Confirm mapping coverage improves in Data Quality.
"""

onboarding_permissions = """If you don’t see onboarding controls, you may need admin rights.
"""

calc_costs = """- **Formula**: Total cost = **Workforce Cost** + **Non-Workforce Cost**.
- **Workforce Cost (WF)**: Demand-based costs for Team/Delivery/Contractor C work, priced with team rates and driven by mapped ADO features and **Derived FTE (SWAG)**.
- **Non-Workforce Cost (NWF)**: Invoices, contracts, licenses, MSP, Contractor CS, and additional/infra/travel/cloud costs linked to applications and teams.
"""

scenarios_core = """- **Baseline**: Budget plan.
- **Expected**: Projection derived from ADO demand (Derived FTE / SWAG).
- **Actual**: Actuals where available (often program-level).
"""

mapped_vs_unassigned = """**Mapped** means work and costs are attributed to a specific application group or team.

**Unassigned** means the data could not be attributed (unmapped or out-of-scope) and should be investigated.
"""

program_level_actuals = """Apptio NWF actuals are often program-level. This can cause Actuals to appear at program scope even when app mapping exists.
"""

canonical_cost_model = """NEXT uses a canonical cost model so totals are consistent across **Welcome**, **Dashboard**, and **Insights**. If numbers differ, check scope, scenario, and mapping coverage. See Getting Started for interpretation guidance.
"""

common_tasks_map_apps = """- Go to **Applications → Onboard**.
- Map unmapped ADO app names to Application Groups.
- Re-check Data Quality coverage.
"""

common_tasks_mapping_coverage = """- Open **Data Quality** and focus on mapping-related issues.
- Resolve unmapped apps/teams, then re-run coverage checks.
"""

common_tasks_expected = """- Verify ADO signals (features, SWAG/Derived FTE, PI Roadmap).
- Check for missing ADO app names or ownership fields.
"""

common_tasks_actuals = """- Confirm Apptio actuals are loaded in Settings.
- Review invoices and contract coverage.
- Use Data Quality to see actuals coverage notes.
"""

dq_common_reasons = """- Incomplete mappings (apps/teams/programs).
- Program-level actuals that can’t be allocated to apps.
- Timing differences between ADO and financial actuals.
- Missing updates to rates, headcount, or ownership.
"""

dq_coverage = """Coverage indicates how much work/cost is attributed to the correct app groups or teams. Low coverage points to mapping gaps.
"""

dq_remediation = """Recommended remediation sequence:
1) Fix mappings → 2) Validate rates/headcount → 3) Re-check Data Quality.
"""

faq_actuals = "Actuals are often sourced at program level and may not map to app groups."
faq_unassigned = "Unassigned means unmapped or out-of-scope work; it is not a real application group."
faq_expected_zero = "Expected can be zero when SWAG/Derived FTE or ADO app names are missing."

page_guide = """- **Welcome**: Executive snapshot of totals and coverage.
- **Dashboard**: Deep dives by program/team/application group.
- **Insights**: Governance signals and action cues.
- **Programs**: Structure and program onboarding.
- **Teams**: Team onboarding, ownership, and headcount.
- **Applications**: App group onboarding and mapping.
- **Rates**: Workforce pricing inputs.
- **Settings**: ADO sync, mappings, and actuals imports.
- **Data Quality**: Coverage diagnostics and remediation.
"""

blocks: list[dict[str, str]] = [
    {
        "id": "gs_purpose",
        "tab": "getting_started",
        "title": "What is NEXT?",
        "body": what_is_next,
        "kind": "markdown",
    },
    {
        "id": "gs_interpretation",
        "tab": "getting_started",
        "title": "How to Interpret NEXT Outputs",
        "body": interpretation_disclaimer,
        "kind": "markdown",
    },
    {
        "id": "gs_po_role",
        "tab": "getting_started",
        "title": "Role of Product Owners in NEXT",
        "body": po_role,
        "kind": "markdown",
    },
    {
        "id": "gs_po_support",
        "tab": "getting_started",
        "title": "How NEXT Supports Product Owners",
        "body": po_support,
        "kind": "markdown",
    },
    {
        "id": "gs_checklist",
        "tab": "getting_started",
        "title": "Getting Started with NEXT",
        "body": getting_started_checklist,
        "kind": "markdown",
    },
    {
        "id": "onboarding_sequence",
        "tab": "next_onboarding",
        "title": "Recommended onboarding sequence",
        "body": onboarding_sequence,
        "kind": "markdown",
    },
    {
        "id": "onboarding_prereqs",
        "tab": "next_onboarding",
        "title": "Prerequisites",
        "body": onboarding_prereqs,
        "kind": "markdown",
    },
    {
        "id": "onboarding_programs",
        "tab": "next_onboarding",
        "title": "Onboard Programs (self-onboard when possible)",
        "body": onboard_programs,
        "kind": "markdown",
    },
    {
        "id": "onboarding_teams",
        "tab": "next_onboarding",
        "title": "Onboard Teams (by Program)",
        "body": onboard_teams,
        "kind": "markdown",
    },
    {
        "id": "onboarding_apps",
        "tab": "next_onboarding",
        "title": "Onboard Applications (ADO app names → Application Groups)",
        "body": onboard_apps,
        "kind": "markdown",
    },
    {
        "id": "onboarding_permissions",
        "tab": "next_onboarding",
        "title": "If you don’t see onboarding controls",
        "body": onboarding_permissions,
        "kind": "info",
    },
    {
        "id": "core_scenarios",
        "tab": "core_concepts",
        "title": "Baseline vs Expected vs Actual",
        "body": scenarios_core,
        "kind": "markdown",
    },
    {
        "id": "core_calc",
        "tab": "core_concepts",
        "title": "How NEXT Calculates Cost",
        "body": calc_costs,
        "kind": "markdown",
    },
    {
        "id": "core_mapped",
        "tab": "core_concepts",
        "title": "Mapped vs Unassigned",
        "body": mapped_vs_unassigned,
        "kind": "markdown",
    },
    {
        "id": "core_actuals",
        "tab": "core_concepts",
        "title": "Program-level Actuals",
        "body": program_level_actuals,
        "kind": "markdown",
    },
    {
        "id": "core_canonical",
        "tab": "core_concepts",
        "title": "Canonical Cost Model",
        "body": canonical_cost_model,
        "kind": "markdown",
    },
    {
        "id": "task_map_apps",
        "tab": "common_tasks",
        "title": "Map unmapped applications",
        "body": common_tasks_map_apps,
        "kind": "expander",
    },
    {
        "id": "task_mapping",
        "tab": "common_tasks",
        "title": "Improve mapping coverage",
        "body": common_tasks_mapping_coverage,
        "kind": "expander",
    },
    {
        "id": "task_expected",
        "tab": "common_tasks",
        "title": "Investigate Expected changes",
        "body": common_tasks_expected,
        "kind": "expander",
    },
    {
        "id": "task_actuals",
        "tab": "common_tasks",
        "title": "Validate Actual coverage",
        "body": common_tasks_actuals,
        "kind": "expander",
    },
    {
        "id": "dq_variances",
        "tab": "data_quality",
        "title": "Common reasons for variances",
        "body": dq_common_reasons,
        "kind": "markdown",
    },
    {
        "id": "dq_coverage",
        "tab": "data_quality",
        "title": "What “coverage” means",
        "body": dq_coverage,
        "kind": "markdown",
    },
    {
        "id": "dq_remediation",
        "tab": "data_quality",
        "title": "Suggested remediation sequence",
        "body": dq_remediation,
        "kind": "markdown",
    },
    {
        "id": "faq_actuals",
        "tab": "faq",
        "title": "Why do Actuals show only at program level?",
        "body": faq_actuals,
        "kind": "expander",
    },
    {
        "id": "faq_unassigned",
        "tab": "faq",
        "title": "What does Unassigned mean?",
        "body": faq_unassigned,
        "kind": "expander",
    },
    {
        "id": "faq_expected_zero",
        "tab": "faq",
        "title": "Why can Expected be zero?",
        "body": faq_expected_zero,
        "kind": "expander",
    },
    {
        "id": "guide_pages",
        "tab": "page_guide",
        "title": "Page-by-Page Guide",
        "body": page_guide,
        "kind": "markdown",
    },
]

def _render_blocks(tab_key: str, focus_id: str = "") -> bool:
    tab_blocks = [b for b in blocks if b["tab"] == tab_key]
    matches = tab_blocks
    used_focus = False

    for block in matches:
        title = block["title"]
        body = block["body"]
        kind = block["kind"]
        if kind == "markdown":
            if title:
                st.markdown(f"#### {title}")
            st.markdown(body)
        elif kind == "expander":
            is_focus = bool(focus_id and block.get("id") == focus_id)
            used_focus = used_focus or is_focus
            with st.expander(title, expanded=is_focus):
                st.markdown(body)
        elif kind == "info":
            st.info(body)
        elif kind == "warning":
            st.warning(body)
        elif kind == "success":
            st.success(body)
    return used_focus


tab_labels = [label for _, label in TAB_SPECS]
current_label = st.session_state.get("howto_selected_tab_label", TAB_LABELS.get(DEFAULT_TAB, tab_labels[0]))
if current_label not in tab_labels:
    current_label = TAB_LABELS.get(DEFAULT_TAB, tab_labels[0])
    st.session_state["howto_selected_tab_label"] = current_label
if hasattr(st, "segmented_control"):
    st.segmented_control(
        "Section",
        options=tab_labels,
        default=current_label,
        key="howto_selected_tab_label",
    )
else:
    st.radio(
        "Section",
        options=tab_labels,
        horizontal=True,
        index=tab_labels.index(current_label),
        key="howto_selected_tab_label",
    )

selected_label = st.session_state.get("howto_selected_tab_label", TAB_LABELS.get(DEFAULT_TAB, tab_labels[0]))
selected_tab = TAB_LABEL_TO_KEY.get(selected_label, DEFAULT_TAB)
st.session_state["howto_selected_tab"] = selected_tab

focus_id = st.session_state.get("howto_focus_id", "")
used_focus = _render_blocks(selected_tab, focus_id=focus_id)
if used_focus:
    st.session_state["howto_focus_id"] = None

st.divider()

# Release Notes (bottom)
release_notes = [
    {
        "version": "0.6a (2025-11-30)",
        "html": """
        <style>
        .rn-table { width: 100%; border-collapse: collapse; font-size: 0.9rem; }
        .rn-table thead tr { background-color: #0f172a; color: #f9fafb; }
        .rn-table th, .rn-table td { padding: 0.6rem 0.75rem; vertical-align: top; border-bottom: 1px solid #e5e7eb; }
        .rn-table th { text-align: left; }
        .rn-table tbody tr:nth-child(even) { background-color: #f9fafb; }
        .rn-module { font-weight: 600; white-space: nowrap; }
        .rn-badge { display: inline-block; padding: 0.15rem 0.5rem; margin-left: 0.35rem; border-radius: 9999px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; }
        .rn-badge-done { background-color: #16a34a1a; color: #166534; border: 1px solid #16a34a; }
        </style>
        <table class="rn-table">
          <thead>
            <tr>
              <th style="width: 22%;">Module</th>
              <th>Update Summary</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td class="rn-module">
                CORE
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Added APPTIO data source integration to retrieve program actuals.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Dashboard
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                New Actuals vs Forecasts chart for APPTIO raw data.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Application
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Added a BASE/Others application group type to correctly allocate feature costs.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Settings
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                New Reconciliation tab with NEXT accuracy KPI across ADO and other integrations.
              </td>
            </tr>
          </tbody>
        </table>
        """
    },
    {
        "version": "0.6 (2025-11-28)",
        "html": """
        <style>
        .rn-table { width: 100%; border-collapse: collapse; font-size: 0.9rem; }
        .rn-table thead tr { background-color: #0f172a; color: #f9fafb; }
        .rn-table th, .rn-table td { padding: 0.6rem 0.75rem; vertical-align: top; border-bottom: 1px solid #e5e7eb; }
        .rn-table th { text-align: left; }
        .rn-table tbody tr:nth-child(even) { background-color: #f9fafb; }
        .rn-module { font-weight: 600; white-space: nowrap; }
        .rn-badge { display: inline-block; padding: 0.15rem 0.5rem; margin-left: 0.35rem; border-radius: 9999px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; }
        .rn-badge-done { background-color: #16a34a1a; color: #166534; border: 1px solid #16a34a; }
        .rn-badge-mixed { background-color: #f973161a; color: #9a3412; border: 1px solid #f97316; }
        .rn-badge-planned { background-color: #eab3081a; color: #854d0e; border: 1px solid #eab308; }
        </style>
        <table class="rn-table">
          <thead>
            <tr>
              <th style="width: 22%;">Module</th>
              <th>Update Summary</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td class="rn-module">
                Dashboard &amp; Welcome
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Updated charts to include Program Additional Costs (monthly → PI split) so treemaps and breakdowns reflect new NWF cost types.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Rates
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Rates now retrieved by cost location (not per-team/program); added additional tabs for clearer layout and per-location editing.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Teams &amp; Programs
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Updated pages to use new cost location components; added Headcount session and expanded change logs for Team/Delivery/Contractor (/CS, /C).
              </td>
            </tr>
          </tbody>
        </table>
        """
    },
    {
        "version": "0.5 (2025-11-20)",
        "html": """
        <style>
        .rn-table { width: 100%; border-collapse: collapse; font-size: 0.9rem; }
        .rn-table thead tr { background-color: #0f172a; color: #f9fafb; }
        .rn-table th, .rn-table td { padding: 0.6rem 0.75rem; vertical-align: top; border-bottom: 1px solid #e5e7eb; }
        .rn-table th { text-align: left; }
        .rn-table tbody tr:nth-child(even) { background-color: #f9fafb; }
        .rn-module { font-weight: 600; white-space: nowrap; }
        .rn-badge { display: inline-block; padding: 0.15rem 0.5rem; margin-left: 0.35rem; border-radius: 9999px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; }
        .rn-badge-done { background-color: #16a34a1a; color: #166534; border: 1px solid #16a34a; }
        .rn-badge-mixed { background-color: #f973161a; color: #9a3412; border: 1px solid #f97316; }
        .rn-badge-planned { background-color: #eab3081a; color: #854d0e; border: 1px solid #eab308; }
        </style>
        <table class="rn-table">
          <thead>
            <tr>
              <th style="width: 22%;">Module</th>
              <th>Update Summary</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td class="rn-module">
                Dashboard
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Added Detailed Cost section (Program, Team, Application, Cost Type, Category, Feature Dimension, Total Cost sorted by Total Cost);
                updated page title to <strong>Dashboard</strong> only; fixed KPI layout by removing extra rows;
                R&amp;M Reports section with program-based filters is planned.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Edit → Access Control
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Reordered <strong>Add Users Manually</strong> to the bottom; formatted <strong>Last Login</strong> as Date + Hour;
                improved user search so unique names return correct email options and user selection no longer auto-selects all results.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Edit → Schema
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Moved <strong>Schema</strong> section to the end; updated diagrams to reflect current SQL tables/views;
                removed <strong>Rollover</strong> and <strong>Rollback</strong> tabs.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Edit → Bulk Edit
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Fixed issue where page refresh prevented bulk changes from being saved.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Edit → Entities
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Added <strong>Access Control → Users</strong> entity.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Rates
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Moved <strong>Preview Calculations</strong> tab to last position;
                rate history per PI/Year is under consideration for future enhancements.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Rates → MSP
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Fixed duplicate primary key error when assigning multiple app groups to an MSP team;
                updated MSP assignments to show <strong>Team Name</strong> only (removed Team ID and Group ID).
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Rates → Teams
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Removed seed button from current teams list; merged team composition into the <strong>Teams (non-MSP)</strong> section;
                removed the duplicated section from the Rates page.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Rates → Edit Rates
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Renamed “current rates” table to <strong>Team Rates</strong> and grouped it with per-team rates;
                grouped <strong>Program Rates</strong> tables together; removed seed default rates button.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Applications → Explore &amp; Link
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Fixed SQL MERGE error in MSP assignment; corrected SQL syntax for transferring ownership of application groups;
                removed the <strong>All Applications</strong> table.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Invoice Tracking → Contracts
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Removed duplicate delete-by-contract option (already available in Edit Contract);
                removed Contract ID, App ID, and Team ID fields from the UI;
                added initial filter by Program in <strong>Edit Contract</strong>;
                prevented duplicate contract creation for the same Application/Application Instance;
                improved layout for Create and Edit Contract forms.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Invoice Tracking → Invoices
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Renamed tab from <strong>Tracking</strong> to <strong>Invoices</strong>;
                updated “Responsible” to display the user’s name instead of email;
                improved filtering so invoices show only the connected user’s Program and Team.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                Welcome
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Added version and release notes section; removed Light/Dark theme toggle;
                fixed “Invoices This Month” calculation; ensured MSP values appear correctly in charts;
                replaced “Unknown” Feature Investment Dimension with <strong>Not Applicable</strong>;
                included MSP in cost breakdown charts.
              </td>
            </tr>
            <tr>
              <td class="rn-module">
                General
                <span class="rn-badge rn-badge-done">Done</span>
              </td>
              <td>
                Created a unified <strong>Change Log</strong> table for Invoice Tracking, Programs, Teams, Vendor, Application, and Rates pages,
                with columns: <strong>Updated At</strong>, <strong>User</strong>, <strong>Changes</strong>.
              </td>
            </tr>
          </tbody>
        </table>
        """
    },
]

st.subheader("Release Notes")
for note in release_notes:
    with st.expander(note["version"], expanded=False):
        st.markdown(note["html"], unsafe_allow_html=True)
