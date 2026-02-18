#!/usr/bin/env python3
"""Offline snapshot contract checker.

Compares the newest and previous local snapshots under data/ or snapshots/.
Generates markdown report with schema drift and growth risks.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import difflib
import json
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".json", ".jsonl", ".parquet", ".db", ".sqlite", ".sqlite3"}
DEFAULT_ROOTS = ("data", "snapshots")
RISK_TABLE_PATTERNS = [
    re.compile(r"ado", re.IGNORECASE),
    re.compile(r"azure[_\- ]?devops", re.IGNORECASE),
    re.compile(r"feature", re.IGNORECASE),
]
RISK_COLUMN_PATTERNS = [
    re.compile(r"nwf", re.IGNORECASE),
    re.compile(r"apptio", re.IGNORECASE),
    re.compile(r"category", re.IGNORECASE),
]


@dataclass
class TableSnapshot:
    name: str
    source_path: str
    row_count: Optional[int]
    columns: Dict[str, str]


@dataclass
class SnapshotCandidate:
    path: Path
    kind: str
    last_modified: float
    files: List[Path] = field(default_factory=list)


@dataclass
class SnapshotModel:
    label: str
    path: Path
    tables: Dict[str, TableSnapshot]


def _safe_rel(path: Path, base: Path) -> str:
    try:
        return str(path.relative_to(base))
    except ValueError:
        return str(path)


def _normalized_type_from_values(values: Sequence[Any]) -> str:
    if not values:
        return "empty"

    non_null = [v for v in values if v not in (None, "")]
    if not non_null:
        return "empty"

    def is_int(v: Any) -> bool:
        try:
            if isinstance(v, bool):
                return False
            int(str(v))
            return str(v).strip() == str(int(str(v)))
        except Exception:
            return False

    def is_float(v: Any) -> bool:
        try:
            if isinstance(v, bool):
                return False
            float(str(v))
            return True
        except Exception:
            return False

    def is_bool(v: Any) -> bool:
        return str(v).strip().lower() in {"true", "false", "0", "1", "yes", "no"}

    def is_date(v: Any) -> bool:
        s = str(v).strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
            try:
                dt.datetime.strptime(s, fmt)
                return True
            except ValueError:
                continue
        return False

    def is_datetime(v: Any) -> bool:
        s = str(v).strip().replace("Z", "+00:00")
        try:
            dt.datetime.fromisoformat(s)
            return True
        except ValueError:
            return False

    if all(is_int(v) for v in non_null):
        return "integer"
    if all(is_float(v) for v in non_null):
        return "float"
    if all(is_bool(v) for v in non_null):
        return "boolean"
    if all(is_datetime(v) for v in non_null):
        return "datetime"
    if all(is_date(v) for v in non_null):
        return "date"

    sample = {type(v).__name__ for v in non_null}
    if len(sample) > 1:
        return "mixed"
    return "string"


def _infer_csv_schema(file_path: Path, delimiter: str, sample_size: int) -> Tuple[Dict[str, str], int]:
    with file_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        fieldnames = reader.fieldnames or []
        samples: Dict[str, List[Any]] = {name: [] for name in fieldnames}
        row_count = 0
        for row in reader:
            row_count += 1
            if row_count <= sample_size:
                for col in fieldnames:
                    samples[col].append(row.get(col))
    schema = {col: _normalized_type_from_values(samples[col]) for col in fieldnames}
    return schema, row_count


def _infer_json_schema(file_path: Path, sample_size: int) -> Tuple[Dict[str, str], int]:
    rows: List[Dict[str, Any]] = []
    with file_path.open("r", encoding="utf-8", errors="ignore") as f:
        first_char = f.read(1)
        f.seek(0)
        if first_char == "[":
            data = json.load(f)
            for item in data:
                if isinstance(item, dict):
                    rows.append(item)
        else:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    rows.append(item)

    row_count = len(rows)
    keys: List[str] = sorted({k for row in rows for k in row.keys()})
    schema: Dict[str, str] = {}
    for key in keys:
        values = [row.get(key) for row in rows[:sample_size]]
        schema[key] = _normalized_type_from_values(values)
    return schema, row_count


def _infer_parquet_schema(file_path: Path) -> Tuple[Dict[str, str], Optional[int]]:
    try:
        import pandas as pd  # type: ignore
    except Exception:
        return {}, None

    try:
        df = pd.read_parquet(file_path)
    except Exception:
        return {}, None

    schema = {str(c): str(t) for c, t in df.dtypes.items()}
    return schema, int(len(df.index))


def _infer_sqlite_tables(file_path: Path) -> Dict[str, TableSnapshot]:
    tables: Dict[str, TableSnapshot] = {}
    conn = sqlite3.connect(str(file_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        for r in rows:
            table = r["name"]
            pragma = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
            columns = {str(col["name"]): str(col["type"] or "unknown") for col in pragma}
            count_row = conn.execute(f"SELECT COUNT(*) AS n FROM '{table}'").fetchone()
            row_count = int(count_row["n"]) if count_row else None
            tables[table] = TableSnapshot(
                name=table,
                source_path=str(file_path),
                row_count=row_count,
                columns=columns,
            )
    finally:
        conn.close()
    return tables


def _collect_snapshot_candidates(base_dir: Path) -> List[SnapshotCandidate]:
    candidates: List[SnapshotCandidate] = []
    for root_name in DEFAULT_ROOTS:
        root = base_dir / root_name
        if not root.exists() or not root.is_dir():
            continue

        for entry in sorted(root.iterdir()):
            if entry.is_dir():
                files = [
                    p
                    for p in entry.rglob("*")
                    if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
                ]
                if files:
                    last_modified = max(p.stat().st_mtime for p in files)
                    candidates.append(
                        SnapshotCandidate(path=entry, kind="directory", last_modified=last_modified, files=files)
                    )
            elif entry.is_file() and entry.suffix.lower() in SUPPORTED_EXTENSIONS:
                stat = entry.stat()
                candidates.append(
                    SnapshotCandidate(path=entry, kind="file", last_modified=stat.st_mtime, files=[entry])
                )
    return sorted(candidates, key=lambda c: c.last_modified, reverse=True)


def _table_name_from_file(snapshot_path: Path, file_path: Path) -> str:
    if snapshot_path.is_file():
        return snapshot_path.stem
    rel = _safe_rel(file_path, snapshot_path)
    return str(Path(rel).with_suffix(""))


def _read_table_file(file_path: Path, snapshot_path: Path, sample_size: int) -> Optional[TableSnapshot]:
    suffix = file_path.suffix.lower()
    table_name = _table_name_from_file(snapshot_path, file_path)

    if suffix == ".csv":
        cols, rows = _infer_csv_schema(file_path, delimiter=",", sample_size=sample_size)
        return TableSnapshot(table_name, str(file_path), rows, cols)
    if suffix == ".tsv":
        cols, rows = _infer_csv_schema(file_path, delimiter="\t", sample_size=sample_size)
        return TableSnapshot(table_name, str(file_path), rows, cols)
    if suffix in {".json", ".jsonl"}:
        cols, rows = _infer_json_schema(file_path, sample_size=sample_size)
        return TableSnapshot(table_name, str(file_path), rows, cols)
    if suffix == ".parquet":
        cols, rows = _infer_parquet_schema(file_path)
        return TableSnapshot(table_name, str(file_path), rows, cols)

    return None


def _build_snapshot_model(candidate: SnapshotCandidate, sample_size: int) -> SnapshotModel:
    tables: Dict[str, TableSnapshot] = {}

    if candidate.path.is_file() and candidate.path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
        tables = _infer_sqlite_tables(candidate.path)
    else:
        for file_path in candidate.files:
            if file_path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
                sqlite_tables = _infer_sqlite_tables(file_path)
                for tname, ts in sqlite_tables.items():
                    tables[f"{file_path.stem}/{tname}"] = ts
                continue
            t = _read_table_file(file_path, candidate.path, sample_size)
            if t:
                tables[t.name] = t

    label = candidate.path.name
    return SnapshotModel(label=label, path=candidate.path, tables=tables)


def _is_risk_area(table_name: str, column_name: Optional[str] = None) -> bool:
    if any(p.search(table_name) for p in RISK_TABLE_PATTERNS):
        return True
    if column_name and any(p.search(column_name) for p in RISK_COLUMN_PATTERNS):
        return True
    return False


def _detect_renames(old_only: Dict[str, str], new_only: Dict[str, str]) -> List[Tuple[str, str, float]]:
    matches: List[Tuple[str, str, float]] = []
    used_new: set[str] = set()
    for old_col, old_type in old_only.items():
        best_new = None
        best_score = 0.0
        for new_col, new_type in new_only.items():
            if new_col in used_new:
                continue
            name_score = difflib.SequenceMatcher(None, old_col.lower(), new_col.lower()).ratio()
            type_bonus = 0.10 if old_type == new_type else 0.0
            score = name_score + type_bonus
            if score > best_score:
                best_score = score
                best_new = new_col
        if best_new and best_score >= 0.72:
            matches.append((old_col, best_new, round(best_score, 3)))
            used_new.add(best_new)
    return matches


def _markdown_table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> List[str]:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(v) for v in row) + " |")
    return out


def _render_report(
    old_model: SnapshotModel,
    new_model: SnapshotModel,
    removed_columns: List[Tuple[str, str, str]],
    renamed_columns: List[Tuple[str, str, str, str, float]],
    type_changes: List[Tuple[str, str, str, str]],
    growth_alerts: List[Tuple[str, int, int, float]],
    output_path: Path,
    growth_threshold: float,
) -> None:
    now = dt.datetime.now().isoformat(timespec="seconds")

    old_tables = set(old_model.tables.keys())
    new_tables = set(new_model.tables.keys())
    removed_tables = sorted(old_tables - new_tables)
    added_tables = sorted(new_tables - old_tables)

    risk_hits: List[Tuple[str, str, str]] = []
    for table_name in sorted(new_tables | old_tables):
        if _is_risk_area(table_name):
            risk_hits.append((table_name, "table", "ADO feature dump pattern"))
    for table_name, col, _ in removed_columns:
        if _is_risk_area(table_name, col):
            risk_hits.append((table_name, col, "NWF/category column removed"))
    for table_name, old_col, new_col, _, _ in renamed_columns:
        if _is_risk_area(table_name, old_col) or _is_risk_area(table_name, new_col):
            risk_hits.append((table_name, f"{old_col} -> {new_col}", "NWF/category rename"))
    for table_name, col, _, _ in type_changes:
        if _is_risk_area(table_name, col):
            risk_hits.append((table_name, col, "NWF/category type change"))

    lines: List[str] = []
    lines.append("# Snapshot Contract Check (Offline)")
    lines.append("")
    lines.append(f"Generated: `{now}`")
    lines.append(f"Compared newest snapshot: `{new_model.path}`")
    lines.append(f"Against previous snapshot: `{old_model.path}`")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    summary_rows = [
        ("Removed tables", len(removed_tables)),
        ("Added tables", len(added_tables)),
        ("Removed columns", len(removed_columns)),
        ("Potential renames", len(renamed_columns)),
        ("Data type changes", len(type_changes)),
        ("Abnormal row growth alerts", len(growth_alerts)),
        ("Risk-area hits", len(risk_hits)),
    ]
    lines.extend(_markdown_table(["Metric", "Count"], summary_rows))
    lines.append("")

    lines.append("## Table-Level Changes")
    lines.append("")
    if removed_tables:
        lines.append("### Removed tables")
        lines.extend([f"- `{t}`" for t in removed_tables])
        lines.append("")
    if added_tables:
        lines.append("### Added tables")
        lines.extend([f"- `{t}`" for t in added_tables])
        lines.append("")
    if not removed_tables and not added_tables:
        lines.append("No table additions/removals detected.")
        lines.append("")

    lines.append("## Column Contract Changes")
    lines.append("")
    if removed_columns:
        lines.append("### Removed columns")
        lines.extend(
            _markdown_table(
                ["Table", "Column", "Old Type"],
                [(t, c, ty) for t, c, ty in removed_columns],
            )
        )
        lines.append("")

    if renamed_columns:
        lines.append("### Potential renamed columns")
        lines.extend(
            _markdown_table(
                ["Table", "From", "To", "Type", "Confidence"],
                [(t, old, new, ty, score) for t, old, new, ty, score in renamed_columns],
            )
        )
        lines.append("")

    if type_changes:
        lines.append("### Data type changes")
        lines.extend(
            _markdown_table(
                ["Table", "Column", "Old Type", "New Type"],
                [(t, c, old_t, new_t) for t, c, old_t, new_t in type_changes],
            )
        )
        lines.append("")

    if not removed_columns and not renamed_columns and not type_changes:
        lines.append("No column-level contract changes detected.")
        lines.append("")

    lines.append("## Row Growth Anomalies")
    lines.append("")
    if growth_alerts:
        lines.extend(
            _markdown_table(
                ["Table", "Previous Rows", "Newest Rows", "Growth Ratio"],
                [(t, old_n, new_n, f"{ratio:.2f}x") for t, old_n, new_n, ratio in growth_alerts],
            )
        )
        lines.append("")
    else:
        lines.append(f"No abnormal row growth above `{growth_threshold:.2f}x` detected.")
        lines.append("")

    lines.append("## Risk Area Focus")
    lines.append("")
    lines.append("Checks are biased toward:")
    lines.append("- ADO feature dump datasets (name patterns like `ado`, `azure devops`, `feature`)")
    lines.append("- Apptio NWF/category columns (patterns like `nwf`, `apptio`, `category`)")
    lines.append("")
    if risk_hits:
        deduped = sorted(set(risk_hits))
        lines.extend(_markdown_table(["Table", "Column/Scope", "Reason"], deduped))
        lines.append("")
    else:
        lines.append("No explicit risk-area hits detected by pattern.")
        lines.append("")

    lines.append("## Migration Suggestions (No Code Changes Applied)")
    lines.append("")
    suggestions: List[str] = []
    if removed_columns:
        suggestions.append("Create backward-compatible aliases/views for removed columns before removing downstream references.")
    if renamed_columns:
        suggestions.append("Introduce dual-write or dual-read compatibility (`old_name` and `new_name`) for one release window.")
    if type_changes:
        suggestions.append("Add explicit cast/normalization steps in ingestion pipelines and validate null/error rates before cutover.")
    if growth_alerts:
        suggestions.append("Review ingest deduplication keys and snapshot extraction filters for growth outliers.")
    if risk_hits:
        suggestions.append("Prioritize contract tests for ADO feature and Apptio NWF/category fields before release.")
    if not suggestions:
        suggestions.append("Keep current schema contract; continue monitoring with this checker for each new snapshot.")

    for s in suggestions:
        lines.append(f"- {s}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(base_dir: Path, output_path: Path, sample_size: int, growth_threshold: float) -> int:
    candidates = _collect_snapshot_candidates(base_dir)
    if len(candidates) < 2:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            "# Snapshot Contract Check (Offline)\n\n"
            "Not enough snapshot candidates found under `data/` and `snapshots/`.\n"
            "Need at least 2 snapshot directories/files to compare.\n",
            encoding="utf-8",
        )
        print(f"Wrote report: {output_path}")
        return 0

    new_candidate = candidates[0]
    old_candidate = candidates[1]

    new_model = _build_snapshot_model(new_candidate, sample_size=sample_size)
    old_model = _build_snapshot_model(old_candidate, sample_size=sample_size)

    removed_columns: List[Tuple[str, str, str]] = []
    renamed_columns: List[Tuple[str, str, str, str, float]] = []
    type_changes: List[Tuple[str, str, str, str]] = []
    growth_alerts: List[Tuple[str, int, int, float]] = []

    shared_tables = sorted(set(old_model.tables) & set(new_model.tables))
    for table_name in shared_tables:
        old_t = old_model.tables[table_name]
        new_t = new_model.tables[table_name]

        old_cols = old_t.columns
        new_cols = new_t.columns

        old_only = {k: v for k, v in old_cols.items() if k not in new_cols}
        new_only = {k: v for k, v in new_cols.items() if k not in old_cols}

        for col, old_type in old_only.items():
            removed_columns.append((table_name, col, old_type))

        rename_matches = _detect_renames(old_only, new_only)
        for old_col, new_col, score in rename_matches:
            old_type = old_only.get(old_col, "unknown")
            renamed_columns.append((table_name, old_col, new_col, old_type, score))
            removed_columns = [r for r in removed_columns if not (r[0] == table_name and r[1] == old_col)]

        for col in set(old_cols.keys()) & set(new_cols.keys()):
            old_type = old_cols[col]
            new_type = new_cols[col]
            if old_type != new_type:
                type_changes.append((table_name, col, old_type, new_type))

        if old_t.row_count is not None and new_t.row_count is not None:
            old_rows = old_t.row_count
            new_rows = new_t.row_count
            if old_rows == 0 and new_rows > 0:
                growth_alerts.append((table_name, old_rows, new_rows, float("inf")))
            elif old_rows > 0:
                ratio = new_rows / old_rows
                if ratio >= growth_threshold and (new_rows - old_rows) >= 100:
                    growth_alerts.append((table_name, old_rows, new_rows, ratio))

    _render_report(
        old_model=old_model,
        new_model=new_model,
        removed_columns=sorted(removed_columns),
        renamed_columns=sorted(renamed_columns),
        type_changes=sorted(type_changes),
        growth_alerts=sorted(growth_alerts),
        output_path=output_path,
        growth_threshold=growth_threshold,
    )

    print(f"Wrote report: {output_path}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline snapshot schema contract checker")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("."),
        help="Repository root that contains data/ and snapshots/",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("automation/reports/snapshot_contract_check.md"),
        help="Output markdown report path",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=200,
        help="Rows sampled for type inference in text formats",
    )
    parser.add_argument(
        "--growth-threshold",
        type=float,
        default=2.0,
        help="Flag row growth at or above this ratio",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run(
        base_dir=args.base_dir.resolve(),
        output_path=args.output,
        sample_size=args.sample_size,
        growth_threshold=args.growth_threshold,
    )


if __name__ == "__main__":
    raise SystemExit(main())
