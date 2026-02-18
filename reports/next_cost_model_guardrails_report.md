# NEXT Cost Model Guardrails Report (Offline)

- Overall: **FAIL**
- Errors: **5**
- Warnings: **0**
- Repo: `/Users/brunobeyer/Documents/GitHub/NEXT`

## 1) Offline dump discovery
- Status: **FAIL**
- No local dump folders found under `data/`, `snapshots/`, or `fixtures/`.

## 2) Loaded sources
- `INVOICES`: missing (not found)
- `APPLICATIONS`: missing (not found)
- `APPLICATION_GROUPS`: missing (not found)
- `VENDORS`: missing (not found)
- `PROGRAM_ADDITIONAL_COSTS`: missing (not found)
- `VW_COSTS_AND_INVOICES`: missing (not found)
- `COSTS_AND_INVOICES`: missing (not found)
- `MAP_ADO_APP_TO_TCO_GROUP`: missing (not found)

## 3) Invariant status
- [FAIL] **INV-001** - Invoices must be tied to valid application instance IDs
  - Details: Required dump tables missing: INVOICES and/or APPLICATIONS.
- [FAIL] **INV-002** - Each application group has a default application instance
  - Details: Required dump tables missing: APPLICATION_GROUPS and/or APPLICATIONS.
- [FAIL] **INV-003** - Vendor mapping consistency across groups/apps/invoices
  - Details: Required dump tables missing: VENDORS, APPLICATION_GROUPS, APPLICATIONS.
- [FAIL] **INV-004** - BASE pool classification consistency
  - Details: APPLICATION_GROUPS dump missing.
- [FAIL] **INV-005** - Program Additional Costs remain program-level and canonical
  - Details: PROGRAM_ADDITIONAL_COSTS dump missing.

## 4) Violations
### 1. INV-001 - Missing required dump tables (ERROR)
- Details: Cannot validate invoice-to-application-instance integrity without INVOICES and APPLICATIONS dumps.
- Suggested minimal patch: `Add local dump exports for INVOICES and APPLICATIONS (CSV/Parquet/JSON/SQLite table) and rerun.`
- References: `db/mssql_backend.py::upsert_invoice`, `db/mssql_backend.py::upsert_application_instance`

### 2. INV-002 - Missing required dump tables (ERROR)
- Details: Cannot validate default instance guardrail without APPLICATION_GROUPS and APPLICATIONS dumps.
- Suggested minimal patch: `Export APPLICATION_GROUPS and APPLICATIONS to local dumps and rerun.`
- References: `db/mssql_backend.py::upsert_application_group`

### 3. INV-003 - Missing required dump tables (ERROR)
- Details: Cannot validate vendor consistency without VENDORS, APPLICATION_GROUPS, and APPLICATIONS dumps.
- Suggested minimal patch: `Add local dumps for VENDORS/APPLICATION_GROUPS/APPLICATIONS and rerun.`
- References: `db/mssql_backend.py::upsert_vendor`, `db/mssql_backend.py::upsert_application_group`, `db/mssql_backend.py::upsert_application_instance`

### 4. INV-004 - Missing APPLICATION_GROUPS dump (ERROR)
- Details: Cannot validate BASE pool classification without APPLICATION_GROUPS.
- Suggested minimal patch: `Export APPLICATION_GROUPS dump and rerun.`
- References: `core/bulkload_v2.py::_validate_required`, `core/bulkload_v2.py (IS_BASE per-team rule)`

### 5. INV-005 - Missing PROGRAM_ADDITIONAL_COSTS dump (ERROR)
- Details: Cannot validate program-level additional cost behavior without PROGRAM_ADDITIONAL_COSTS.
- Suggested minimal patch: `Export PROGRAM_ADDITIONAL_COSTS dump and rerun.`
- References: `db/mssql_backend.py::ensure_program_additional_costs_view`, `core/quality/policy.py::is_app_group_mapping_required`

## 5) Minimal patch strategy
- Apply only targeted data-fix patches for listed violations; do not refactor canonical cost logic.
- Re-run this offline guardrail after each patch and stop when all errors are cleared.
