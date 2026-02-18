Purpose

This repository is maintained with the help of AI assistants (Codex / GPT).

This document defines how AI tools must modify the code safely, without breaking the financial logic, pages, or user experience.

AI editors must:
	•	respect the architecture
	•	avoid unnecessary refactors
	•	change only explicitly requested areas
	•	prefer diagnostics over assumptions
	•	keep code readable and consistent

If there is uncertainty, stop and add comments instead of guessing.

⸻

	1.	Architectural Overview (DO NOT BREAK)

1.1 Canonical Cost Engine — Single Source of Truth

All costs in the app must come from:

core/canonical_costs.py

Only these functions are allowed to produce authoritative cost results:

get_pi_costs
get_app_group_costs
get_feature_costs
get_unassigned_breakdown
get_msp_costs

Rules:

UI pages must call these functions for totals, breakdowns, and diagnostics.
No UI page should compute its own cost math.
No duplicated cost logic anywhere else.

Forbidden:

Rebuilding cost SQL in UI files
Adding new “cost” helpers outside canonical_costs.py
Using CUSTOM_FTE for money

CUSTOM_FTE is for coverage/reference only. Projected costs come exclusively from Derived FTE via SWAG pipelines.

⸻

1.2 Page Roles (by file)

Welcome — Overview

pages/0_Welcome.py

High-level KPIs
Scenario selector (Baseline vs Projected)
Coverage, effort and cost perspective
Uses canonical functions only

Dashboard

pages/1_Dashboard.py

Scenario cost exploration
Distributions, breakdowns, drilldown
No direct cost SQL

Invoices

pages/2_Invoices.py

CRUD for invoices
Diagnostics and unassigned view
No cost calculations beyond diagnostics

Contracts

pages/2_Contracts.py

CRUD for contracts
Diagnostics and unassigned validation

Budget

pages/Budget.py

Baseline budget views only
KPIs and charts use canonical baseline functions
Budget Explorer may fetch raw budget rows only for table detail

Master Data

pages/3_Programs.py
pages/4_Teams.py
pages/5_Vendors.py
pages/6_Applications.py

These pages manage master data only:

IDs
Mappings
Structure
Attributes

Rules:

No cost math
No canonical cost functions except for optional debug
Do not break editing behavior or workflows

Teams special rules:

Headcount edits write to TEAM_HEADCOUNT_HISTORY and TEAM_CONTRACTOR_HEADCOUNT
Composition is recomputed automatically
TEAMS baseline sync must work
Debug expanders are allowed and preferred over logic changes

Rates

pages/7_Rates.py

Maintain workforce rate tables
Preview calculations allowed only if labeled as preview, not authoritative

Data Quality

pages/11_Data_Quality.py

Central diagnostics cockpit. Must use canonical functions.

get_unassigned_breakdown
get_pi_costs
get_app_group_costs
get_msp_costs

Covers:

Unassigned baseline
Unassigned projected
Baseline vs projected checks

No custom cost logic allowed.

Settings

pages/10_Settings.py

Configuration only
No cost logic

Admin

pages/99_Admin.py

Maintenance, sync tools, DB utilities
No cost logic

Visual Lab (if present)

Experimental visuals only
Must explicitly say numbers are not canonical

⸻

	2.	Global Safety Rules

When editing:

Make the smallest change possible
Modify only explicitly related files
Preserve all layouts, tabs, visual structure, filters and navigation
Prefer debug panels before touching logic
Respect page responsibilities and data flow boundaries
Leave comments when unsure

Do NOT:

Refactor things “for style”
Merge unrelated code
Introduce new scenarios
Remove fallback logic
Compute costs manually in UI
Rename important functions unless asked
Delete DB tables, columns or views

⸻

	3.	Scenario Rules

Projected scenario:

Must always be derived from SWAG and Derived FTE logic.
Uses workforce split views.

Never use:

Custom FTE
Raw story points for money conversion

Baseline scenario:

Uses approved budget tables only.
Budget page remains baseline-only.

⸻

	4.	Debugging Philosophy

When something looks wrong:

Do NOT rewrite logic immediately.
Add a Streamlit expander such as:

Debug – diagnostics

Prefer showing:

Raw DB rows
Effective views
Canonical results side by side

Transparency first. Avoid silent fixes.

⸻

	5.	SQL and DB changes

When modifying SQL, functions, pipelines, or DB schemas:

Document WHY
Do not remove columns without explicit instruction
Prefer extending structures rather than replacing them
Ensure compatibility with canonical_costs.py
Validate field names and expected types

If unsure, comment and stop instead of guessing.

⸻

	6.	Allowed AI Actions

AI may:

Add debug expanders
Add charts powered by canonical functions
Add captions, tooltips and warnings
Improve validation messages
Fix clear, contained bugs
Add missing imports
Add documentation comments
Improve user visibility of data problems

⸻

	7.	Forbidden AI Actions

AI must NOT:

Replace canonical function calls with SQL
Inline cost formulas inside UI pages
Change cost logic silently
Blend master data logic with cost logic
Rewrite entire modules unless explicitly requested
Modify unrelated files without reason
Break DB compatibility
Change return structures of existing functions

⸻

	8.	Decision Checklist Before Editing

Before applying a code change, AI must answer:
	1.	Does this page own the cost logic?
If yes, stop. Move logic to canonical_costs instead.
	2.	Does the change change page responsibilities?
If yes, stop.
	3.	Does the change introduce new data flow?
If yes, explain the reasoning and document.
	4.	Can this affect financial accuracy?
Add diagnostics first. Do not guess.

⸻

	9.	If unsure

When uncertain:

Leave comments
Prefer read-only diagnostics
Mark TODO sections instead of dangerous changes
Ask the human user rather than proceeding

⸻

	10.	Golden Rules

Always:

Use canonical_costs
Keep architecture intact
Preserve UX as-is
Prefer minimal changes
Make things observable and debuggable

Never:

Recompute costs locally
Change logic silently
Touch unrelated files
Guess about financial rules
