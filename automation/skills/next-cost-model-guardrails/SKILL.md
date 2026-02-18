---
name: next-cost-model-guardrails
description: Offline static guardrail validation for NEXT canonical cost-model invariants and risky cost-join patterns. Use when validating repository safety for invoices-to-instance integrity, default instance coverage, vendor mapping consistency, BASE pool stability, and program-level Additional Costs without network access.
---

# NEXT Cost Model Guardrails

Run offline guardrail checks over:
- `domain/`
- `cost/`
- `admin/`
- `settings/`

Use only local files. Do not use network access.

## Run

Execute:

```bash
python automation/skills/next-cost-model-guardrails/scripts/run_guardrails.py
```

Optional flags:

```bash
python automation/skills/next-cost-model-guardrails/scripts/run_guardrails.py \
  --repo-root /absolute/path/to/NEXT \
  --output automation/reports/cost_model_guardrails.md
```

## Checks

Validate these invariants from static code evidence:
- invoices remain tied to application instance IDs
- each application group has a default instance when required
- vendor mappings stay consistent
- BASE pool classification does not change unexpectedly
- Additional Costs stay program-level

Detect risk patterns:
- joins that could duplicate costs
- missing instance IDs
- vendor null mappings

## Output Contract

Always write Markdown report to:
- `automation/reports/cost_model_guardrails.md`

Report must include:
- Overall `PASS` or `FAIL`
- Potential risk level: `LOW`, `MEDIUM`, or `HIGH`
- Exact file locations (path and line)

If scoped folders are missing or empty, mark as `FAIL` and include those folder paths as findings.
