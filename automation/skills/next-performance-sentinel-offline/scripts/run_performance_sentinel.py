#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import datetime as dt
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

HEAVY_WEIGHTS: dict[str, int] = {
    "merge": 6,
    "groupby": 5,
    "join": 5,
    "pivot_table": 6,
    "apply": 4,
    "agg": 2,
    "transform": 3,
    "sort_values": 2,
    "reset_index": 1,
    "assign": 1,
    "melt": 2,
    "explode": 2,
}

CACHE_DECORATORS = {
    "cache_data_portfolio",
    "cache_resource_portfolio",
    "cache_data",
    "cache_resource",
    "lru_cache",
}

DEFAULT_PAGES = [
    "pages/0_Welcome.py",
    "pages/1_Dashboard.py",
    "pages/1_Insights.py",
]


@dataclass
class FunctionMetric:
    module: str
    function: str
    file_path: Path
    lineno: int
    op_counts: Counter
    chains: list[tuple[str, ...]]
    repeated_chain_count: int
    expensive_chain_count: int
    has_cache: bool
    approx_score: float
    approx_ms: float


class PageCallCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.from_imports: dict[str, tuple[str, str]] = {}
        self.module_aliases: dict[str, str] = {}
        self.calls: set[tuple[str, str]] = set()

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if not node.module:
            return
        if not node.module.startswith("core"):
            return
        for alias in node.names:
            local = alias.asname or alias.name
            self.from_imports[local] = (node.module, alias.name)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name.startswith("core"):
                local = alias.asname or alias.name
                self.module_aliases[local] = alias.name

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Name):
            hit = self.from_imports.get(func.id)
            if hit:
                self.calls.add(hit)
        elif isinstance(func, ast.Attribute):
            val = func.value
            if isinstance(val, ast.Name) and val.id in self.module_aliases:
                self.calls.add((self.module_aliases[val.id], func.attr))
            elif isinstance(val, ast.Attribute):
                mod = self._flatten_attr_module(val)
                if mod and mod.startswith("core"):
                    self.calls.add((mod, func.attr))
        self.generic_visit(node)

    @staticmethod
    def _flatten_attr_module(node: ast.Attribute) -> Optional[str]:
        parts: list[str] = []
        cur: ast.AST = node
        while isinstance(cur, ast.Attribute):
            parts.append(cur.attr)
            cur = cur.value
        if isinstance(cur, ast.Name):
            parts.append(cur.id)
            return ".".join(reversed(parts))
        return None


class FunctionAnalyzer(ast.NodeVisitor):
    def __init__(self) -> None:
        self.op_counts: Counter = Counter()
        self.chains: list[tuple[str, ...]] = []
        self.cache_decorators: set[str] = set()

    def analyze_function(self, fn: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        for dec in fn.decorator_list:
            dec_name = self._name_of_decorator(dec)
            if dec_name:
                self.cache_decorators.add(dec_name)
        for node in ast.walk(fn):
            if isinstance(node, ast.Call):
                chain = self._extract_chain(node)
                if chain:
                    normalized = tuple(chain)
                    self.chains.append(normalized)
                    for op in normalized:
                        if op in HEAVY_WEIGHTS:
                            self.op_counts[op] += 1

    @staticmethod
    def _name_of_decorator(node: ast.AST) -> Optional[str]:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        if isinstance(node, ast.Call):
            return FunctionAnalyzer._name_of_decorator(node.func)
        return None

    @staticmethod
    def _extract_chain(call: ast.Call) -> list[str]:
        chain: list[str] = []
        cur: ast.AST = call
        while isinstance(cur, ast.Call):
            fn = cur.func
            if isinstance(fn, ast.Attribute):
                chain.append(fn.attr)
                cur = fn.value
                continue
            break
        chain.reverse()
        if not chain:
            return []
        if len(chain) == 1 and chain[0] not in HEAVY_WEIGHTS:
            return []
        return chain


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Offline NEXT performance sentinel")
    p.add_argument("--repo-root", default=".", help="Path to NEXT repository root")
    p.add_argument("--pages", nargs="*", default=DEFAULT_PAGES, help="Page files to simulate")
    p.add_argument(
        "--output",
        default="automation/reports/performance_sentinel.md",
        help="Markdown output path relative to repo root",
    )
    p.add_argument("--top", type=int, default=20, help="Top functions to report")
    p.add_argument("--cache-threshold", type=float, default=26.0, help="Scaled score threshold")
    return p.parse_args()


def read_ast(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def collect_core_calls(page_path: Path) -> set[tuple[str, str]]:
    tree = read_ast(page_path)
    collector = PageCallCollector()
    collector.visit(tree)
    return collector.calls


def module_to_file(repo_root: Path, module: str) -> Path:
    rel = Path(*module.split("."))
    return repo_root / f"{rel}.py"


def locate_function_def(module_tree: ast.AST, fn_name: str) -> Optional[ast.FunctionDef | ast.AsyncFunctionDef]:
    for node in module_tree.body if isinstance(module_tree, ast.Module) else []:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == fn_name:
            return node
    return None


def compute_scale_rows(repo_root: Path) -> tuple[int, list[tuple[str, int]]]:
    dirs = [repo_root / "data", repo_root / "snapshots", repo_root / "fixtures"]
    file_rows: list[tuple[str, int]] = []
    total = 0
    for d in dirs:
        if not d.exists() or not d.is_dir():
            continue
        for f in d.rglob("*"):
            if not f.is_file():
                continue
            if f.name.startswith("."):
                continue
            ext = f.suffix.lower()
            rows = 0
            try:
                if ext in {".csv", ".tsv", ".txt"}:
                    with f.open("r", encoding="utf-8", errors="ignore") as handle:
                        for _ in handle:
                            rows += 1
                    rows = max(rows - 1, 0)
                elif ext in {".json", ".jsonl"}:
                    with f.open("r", encoding="utf-8", errors="ignore") as handle:
                        for _ in handle:
                            rows += 1
                elif ext in {".parquet", ".feather", ".db", ".sqlite"}:
                    rows = max(int(f.stat().st_size / 220), 1)
                else:
                    rows = max(int(f.stat().st_size / 500), 1)
            except Exception:
                rows = 0
            if rows > 0:
                rel = str(f.relative_to(repo_root))
                file_rows.append((rel, rows))
                total += rows
    file_rows.sort(key=lambda x: x[1], reverse=True)
    return total, file_rows[:12]


def has_cache_decorator(dec_names: Iterable[str]) -> bool:
    for name in dec_names:
        if name in CACHE_DECORATORS:
            return True
        if name.endswith("cache_data") or name.endswith("cache_resource"):
            return True
    return False


def score_function(op_counts: Counter, chain_counts: Counter, scale_rows: int) -> tuple[float, float, int, int]:
    base = 0.0
    for op, cnt in op_counts.items():
        base += HEAVY_WEIGHTS.get(op, 0) * float(cnt)
    repeated = sum(1 for _k, c in chain_counts.items() if c >= 2)
    expensive = 0
    for chain in chain_counts:
        if len(chain) >= 5 and any(op in HEAVY_WEIGHTS for op in chain):
            expensive += 1
        if "groupby" in chain and "merge" in chain:
            expensive += 1
    if op_counts.get("groupby", 0) >= 2 and op_counts.get("merge", 0) >= 1:
        expensive += 1
    base += repeated * 3 + expensive * 2
    scale = 1.0 + (math.log10(max(scale_rows, 1)) / 4.0)
    scaled = base * scale
    approx_ms = scaled * 4.2
    return scaled, approx_ms, repeated, expensive


def analyze_targets(repo_root: Path, calls: set[tuple[str, str]], scale_rows: int) -> list[FunctionMetric]:
    metrics: list[FunctionMetric] = []
    module_cache: dict[str, ast.AST] = {}
    for module, fn_name in sorted(calls):
        mfile = module_to_file(repo_root, module)
        if not mfile.exists():
            continue
        mtree = module_cache.get(module)
        if mtree is None:
            try:
                mtree = read_ast(mfile)
                module_cache[module] = mtree
            except Exception:
                continue
        fn = locate_function_def(mtree, fn_name)
        if not fn:
            continue

        analyzer = FunctionAnalyzer()
        analyzer.analyze_function(fn)
        chains_counter = Counter(analyzer.chains)
        scaled, approx_ms, repeated, expensive = score_function(analyzer.op_counts, chains_counter, scale_rows)

        metrics.append(
            FunctionMetric(
                module=module,
                function=fn_name,
                file_path=mfile,
                lineno=getattr(fn, "lineno", 1),
                op_counts=analyzer.op_counts,
                chains=list(analyzer.chains),
                repeated_chain_count=repeated,
                expensive_chain_count=expensive,
                has_cache=has_cache_decorator(analyzer.cache_decorators),
                approx_score=scaled,
                approx_ms=approx_ms,
            )
        )
    metrics.sort(key=lambda m: (m.approx_score, m.expensive_chain_count), reverse=True)
    return metrics


def collect_repeated_patterns(metrics: Sequence[FunctionMetric]) -> list[tuple[tuple[str, ...], int, list[str]]]:
    pattern_hits: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for m in metrics:
        local_counts = Counter(m.chains)
        for pattern, count in local_counts.items():
            if count >= 2:
                pattern_hits[pattern].append(f"{m.module}.{m.function} x{count}")
    rows = []
    for pattern, owners in pattern_hits.items():
        total = len(owners)
        rows.append((pattern, total, owners))
    rows.sort(key=lambda x: (x[1], len(x[0])), reverse=True)
    return rows


def missing_cache_candidates(metrics: Sequence[FunctionMetric], threshold: float) -> list[FunctionMetric]:
    out = [m for m in metrics if (not m.has_cache) and m.approx_score >= threshold]
    out.sort(key=lambda m: m.approx_score, reverse=True)
    return out


def memoization_candidates(metrics: Sequence[FunctionMetric]) -> list[str]:
    picks = []
    for m in metrics:
        if m.has_cache:
            continue
        if m.approx_score < 18:
            continue
        key_ops = ", ".join(op for op, _ in m.op_counts.most_common(3)) or "general dataframe work"
        picks.append(
            f"`{m.module}.{m.function}` ({key_ops}) - add argument-keyed memoization for repeated page filters."
        )
        if len(picks) >= 8:
            break
    return picks


def preaggregation_candidates(metrics: Sequence[FunctionMetric]) -> list[str]:
    picks = []
    for m in metrics:
        g = m.op_counts.get("groupby", 0)
        mg = m.op_counts.get("merge", 0)
        if g + mg < 3:
            continue
        picks.append(
            f"`{m.module}.{m.function}` - pre-aggregate repeated groupby outputs before merge fan-out (groupby={g}, merge={mg})."
        )
        if len(picks) >= 8:
            break
    return picks


def write_report(
    output_path: Path,
    *,
    pages: Sequence[Path],
    calls: Sequence[tuple[str, str]],
    scale_rows: int,
    top_files: Sequence[tuple[str, int]],
    metrics: Sequence[FunctionMetric],
    repeated_patterns: Sequence[tuple[tuple[str, ...], int, list[str]]],
    cache_misses: Sequence[FunctionMetric],
    memos: Sequence[str],
    preaggs: Sequence[str],
    top_n: int,
) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: list[str] = []
    lines.append("# NEXT Performance Sentinel (Offline)")
    lines.append("")
    lines.append(f"Generated: {ts}")
    lines.append("")
    lines.append("## Execution Mode")
    lines.append("")
    lines.append("- Offline-only: Yes")
    lines.append("- External APIs called: No")
    lines.append("- Azure DevOps called: No")
    lines.append("- App logic modified: No")
    lines.append("")
    lines.append("## Pipeline Simulation Scope")
    lines.append("")
    for p in pages:
        lines.append(f"- `{p.as_posix()}`")
    lines.append(f"- Detected core function call targets: {len(calls)}")
    lines.append(f"- Analyzed function definitions resolved: {len(metrics)}")
    lines.append("")
    lines.append("## Local Dump Footprint")
    lines.append("")
    lines.append(f"- Approximate total local rows/events: {scale_rows:,}")
    for f, rows in top_files:
        lines.append(f"- `{f}`: ~{rows:,}")
    lines.append("")
    lines.append("## Approximate Cost by Function")
    lines.append("")
    lines.append("| Rank | Function | Approx ms | Score | groupby | merge | Chains | Cache |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |")
    for idx, m in enumerate(metrics[:top_n], start=1):
        lines.append(
            "| "
            f"{idx} | `{m.module}.{m.function}` | {m.approx_ms:.1f} | {m.approx_score:.1f} | "
            f"{m.op_counts.get('groupby', 0)} | {m.op_counts.get('merge', 0)} | {len(m.chains)} | "
            f"{'Yes' if m.has_cache else 'No'} |"
        )
    if not metrics:
        lines.append("No resolvable core functions were analyzed from the selected pages.")
    lines.append("")
    lines.append("## Repeated DataFrame Transformation Patterns")
    lines.append("")
    if repeated_patterns:
        for pattern, _owners_count, owners in repeated_patterns[:10]:
            pat = " -> ".join(pattern)
            lines.append(f"- `{pat}`")
            for owner in owners[:4]:
                lines.append(f"  - {owner}")
    else:
        lines.append("- No repeated chains detected at threshold (>=2 within a function).")
    lines.append("")
    lines.append("## Expensive GroupBy/Merge Chain Findings")
    lines.append("")
    expensive = [m for m in metrics if m.expensive_chain_count > 0]
    if expensive:
        for m in expensive[:top_n]:
            lines.append(
                f"- `{m.module}.{m.function}`: expensive_chains={m.expensive_chain_count}, "
                f"groupby={m.op_counts.get('groupby', 0)}, merge={m.op_counts.get('merge', 0)}"
            )
    else:
        lines.append("- None flagged by current heuristic thresholds.")
    lines.append("")
    lines.append("## Missing Caching Opportunities")
    lines.append("")
    if cache_misses:
        for m in cache_misses[:top_n]:
            lines.append(
                f"- `{m.module}.{m.function}` (score={m.approx_score:.1f}, approx_ms={m.approx_ms:.1f}) "
                "- no cache decorator detected."
            )
    else:
        lines.append("- No high-score uncached functions found at current threshold.")
    lines.append("")
    lines.append("## Suggested Memoization Candidates")
    lines.append("")
    if memos:
        for row in memos:
            lines.append(f"- {row}")
    else:
        lines.append("- No memoization candidates met threshold.")
    lines.append("")
    lines.append("## Suggested Pre-Aggregation Points")
    lines.append("")
    if preaggs:
        for row in preaggs:
            lines.append(f"- {row}")
    else:
        lines.append("- No pre-aggregation candidates met threshold.")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Cost values are approximate ranking heuristics, not benchmark timings.")
    lines.append("- Report is intentionally diagnostic-only; no source code changes are applied.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()

    pages: list[Path] = []
    for p in args.pages:
        abs_p = (repo_root / p).resolve() if not Path(p).is_absolute() else Path(p)
        if abs_p.exists():
            pages.append(abs_p)

    all_calls: set[tuple[str, str]] = set()
    for p in pages:
        all_calls.update(collect_core_calls(p))

    scale_rows, top_files = compute_scale_rows(repo_root)
    metrics = analyze_targets(repo_root, all_calls, scale_rows)
    repeated_patterns = collect_repeated_patterns(metrics)
    cache_misses = missing_cache_candidates(metrics, threshold=args.cache_threshold)
    memos = memoization_candidates(metrics)
    preaggs = preaggregation_candidates(metrics)

    output = (repo_root / args.output).resolve() if not Path(args.output).is_absolute() else Path(args.output)
    write_report(
        output,
        pages=[p.relative_to(repo_root) if p.is_relative_to(repo_root) else p for p in pages],
        calls=sorted(all_calls),
        scale_rows=scale_rows,
        top_files=top_files,
        metrics=metrics,
        repeated_patterns=repeated_patterns,
        cache_misses=cache_misses,
        memos=memos,
        preaggs=preaggs,
        top_n=max(1, args.top),
    )
    print(f"[OK] Wrote report: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
