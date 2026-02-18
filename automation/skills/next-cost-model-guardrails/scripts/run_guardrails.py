#!/usr/bin/env python3
"""Offline cost-model guardrail scanner for NEXT."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


SCAN_DIRS: Sequence[str] = ("domain", "cost", "admin", "settings")
CODE_EXTS = {".py", ".sql", ".md", ".txt", ".yaml", ".yml", ".json", ".toml"}


@dataclass
class Finding:
    kind: str
    severity: str
    message: str
    location: str


def iter_files(base: Path) -> Iterable[Path]:
    for path in base.rglob("*"):
        if path.is_file() and path.suffix.lower() in CODE_EXTS:
            yield path


def rel(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def add(matches: List[Finding], kind: str, severity: str, message: str, file_path: Path, line_no: int, root: Path) -> None:
    matches.append(
        Finding(
            kind=kind,
            severity=severity,
            message=message,
            location=f"{rel(file_path, root)}:{line_no}",
        )
    )


def scan_file(path: Path, root: Path, findings: List[Finding], evidence: Dict[str, bool]) -> None:
    text = path.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()

    t = text.lower()
    if ("invoice" in t or "invoices" in t) and ("application_instance_id" in t or "instance_id" in t):
        evidence["invoice_instance"] = True
    if ("application_group" in t or "group" in t) and ("default_instance" in t or "is_default" in t):
        evidence["default_instance"] = True
    if "vendor" in t and ("mapping" in t or "map_" in t or "vendor_id" in t):
        evidence["vendor_mapping"] = True
    if "base" in t and ("pool" in t or "classification" in t or "is_base" in t):
        evidence["base_pool"] = True
    if ("additional cost" in t or "additional_cost" in t) and "program" in t:
        evidence["program_additional_cost"] = True

    for i, line in enumerate(lines, start=1):
        low = line.lower()
        if "join" in low and ("cost" in low or "invoice" in low):
            if "instance_id" not in low and "application_instance_id" not in low:
                add(
                    findings,
                    kind="duplicate_cost_join_risk",
                    severity="HIGH",
                    message="Join touching cost/invoice data without instance-id key on the join line.",
                    file_path=path,
                    line_no=i,
                    root=root,
                )
        if ("invoice" in low or "invoices" in low) and "instance_id" not in low and "application_instance_id" not in low:
            if re.search(r"\b(select|insert|update|merge|join|where)\b", low):
                add(
                    findings,
                    kind="missing_instance_id_risk",
                    severity="HIGH",
                    message="Invoice-related statement without visible instance-id token on the line.",
                    file_path=path,
                    line_no=i,
                    root=root,
                )
        if "vendor" in low and re.search(r"\bis\s+null\b|=\s*null|none|nullif", low):
            add(
                findings,
                kind="vendor_null_mapping_risk",
                severity="MEDIUM",
                message="Vendor mapping may allow null vendor linkage.",
                file_path=path,
                line_no=i,
                root=root,
            )


def invariant_findings(root: Path, evidence: Dict[str, bool]) -> List[Finding]:
    rules: List[Tuple[str, str]] = [
        ("invoice_instance", "Invoices must remain tied to application instance IDs."),
        ("default_instance", "Each application group should have a default instance if required."),
        ("vendor_mapping", "Vendor mappings must remain consistent."),
        ("base_pool", "BASE pool classification must not change unexpectedly."),
        ("program_additional_cost", "Additional Costs must remain program-level."),
    ]
    out: List[Finding] = []
    for key, msg in rules:
        if not evidence.get(key):
            out.append(
                Finding(
                    kind="invariant_evidence_missing",
                    severity="HIGH",
                    message=f"No static evidence found in scan scope: {msg}",
                    location="domain/, cost/, admin/, settings/",
                )
            )
    return out


def risk_level(findings: Sequence[Finding]) -> str:
    if any(f.severity == "HIGH" for f in findings):
        return "HIGH"
    if any(f.severity == "MEDIUM" for f in findings):
        return "MEDIUM"
    return "LOW"


def render_report(
    root: Path,
    output: Path,
    scope_notes: List[Finding],
    findings: List[Finding],
    evidence: Dict[str, bool],
) -> None:
    all_findings = scope_notes + findings + invariant_findings(root, evidence)
    status = "PASS" if not all_findings else "FAIL"
    risk = risk_level(all_findings)
    output.parent.mkdir(parents=True, exist_ok=True)

    lines: List[str] = []
    lines.append("# Cost Model Guardrails")
    lines.append("")
    lines.append(f"- Overall: **{status}**")
    lines.append(f"- Potential risk level: **{risk}**")
    lines.append(f"- Scan root: `{root}`")
    lines.append("")
    lines.append("## Scope")
    for d in SCAN_DIRS:
        lines.append(f"- `{d}/`")
    lines.append("")
    lines.append("## Invariant Evidence")
    lines.append(f"- Invoice instance linkage evidence: **{'YES' if evidence['invoice_instance'] else 'NO'}**")
    lines.append(f"- Default instance evidence: **{'YES' if evidence['default_instance'] else 'NO'}**")
    lines.append(f"- Vendor mapping evidence: **{'YES' if evidence['vendor_mapping'] else 'NO'}**")
    lines.append(f"- BASE pool evidence: **{'YES' if evidence['base_pool'] else 'NO'}**")
    lines.append(f"- Program-level Additional Costs evidence: **{'YES' if evidence['program_additional_cost'] else 'NO'}**")
    lines.append("")
    lines.append("## Findings")
    if not all_findings:
        lines.append("- None")
    else:
        for idx, f in enumerate(all_findings, start=1):
            lines.append(f"{idx}. [{f.severity}] `{f.kind}` - {f.message}")
            lines.append(f"   - Location: `{f.location}`")

    output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline guardrail checks for NEXT cost model.")
    parser.add_argument("--repo-root", default=".", help="Repository root path.")
    parser.add_argument(
        "--output",
        default="automation/reports/cost_model_guardrails.md",
        help="Markdown output path (relative to repo root or absolute).",
    )
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    output = Path(args.output)
    if not output.is_absolute():
        output = root / output

    scope_notes: List[Finding] = []
    findings: List[Finding] = []
    evidence = {
        "invoice_instance": False,
        "default_instance": False,
        "vendor_mapping": False,
        "base_pool": False,
        "program_additional_cost": False,
    }

    for d in SCAN_DIRS:
        target = root / d
        if not target.exists():
            scope_notes.append(
                Finding(
                    kind="scan_scope_missing",
                    severity="HIGH",
                    message="Required scan folder does not exist.",
                    location=f"{d}/",
                )
            )
            continue
        if not target.is_dir():
            scope_notes.append(
                Finding(
                    kind="scan_scope_not_directory",
                    severity="HIGH",
                    message="Scan scope entry exists but is not a directory.",
                    location=f"{d}/",
                )
            )
            continue
        files = list(iter_files(target))
        if not files:
            scope_notes.append(
                Finding(
                    kind="scan_scope_empty",
                    severity="MEDIUM",
                    message="Scan folder exists but contains no supported files.",
                    location=f"{d}/",
                )
            )
            continue
        for file_path in files:
            scan_file(file_path, root, findings, evidence)

    render_report(root, output, scope_notes, findings, evidence)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
