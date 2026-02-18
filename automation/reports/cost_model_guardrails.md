# Cost Model Guardrails

- Overall: **FAIL**
- Potential risk level: **HIGH**
- Scan root: `/Users/brunobeyer/Documents/GitHub/NEXT`

## Scope
- `domain/`
- `cost/`
- `admin/`
- `settings/`

## Invariant Evidence
- Invoice instance linkage evidence: **NO**
- Default instance evidence: **NO**
- Vendor mapping evidence: **NO**
- BASE pool evidence: **NO**
- Program-level Additional Costs evidence: **NO**

## Findings
1. [MEDIUM] `scan_scope_empty` - Scan folder exists but contains no supported files.
   - Location: `domain/`
2. [MEDIUM] `scan_scope_empty` - Scan folder exists but contains no supported files.
   - Location: `cost/`
3. [HIGH] `scan_scope_missing` - Required scan folder does not exist.
   - Location: `admin/`
4. [HIGH] `scan_scope_missing` - Required scan folder does not exist.
   - Location: `settings/`
5. [HIGH] `invariant_evidence_missing` - No static evidence found in scan scope: Invoices must remain tied to application instance IDs.
   - Location: `domain/, cost/, admin/, settings/`
6. [HIGH] `invariant_evidence_missing` - No static evidence found in scan scope: Each application group should have a default instance if required.
   - Location: `domain/, cost/, admin/, settings/`
7. [HIGH] `invariant_evidence_missing` - No static evidence found in scan scope: Vendor mappings must remain consistent.
   - Location: `domain/, cost/, admin/, settings/`
8. [HIGH] `invariant_evidence_missing` - No static evidence found in scan scope: BASE pool classification must not change unexpectedly.
   - Location: `domain/, cost/, admin/, settings/`
9. [HIGH] `invariant_evidence_missing` - No static evidence found in scan scope: Additional Costs must remain program-level.
   - Location: `domain/, cost/, admin/, settings/`
