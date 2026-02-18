#!/usr/bin/env python3
"""
Repo doctor: quick checks for common Mac/backup artifacts that break Streamlit navigation.

Usage:
  python3 scripts/repo_doctor.py
"""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    problems: list[str] = []

    # 1) Entrypoint
    if not (ROOT / "main.py").is_file():
        problems.append("Missing entrypoint: main.py (expected at repo root)")

    # 2) __MACOSX
    if (ROOT / "__MACOSX").exists():
        problems.append("Found __MACOSX/ at repo root (delete it)")

    # 3) .DS_Store anywhere
    ds = sorted(p for p in ROOT.rglob(".DS_Store") if p.is_file())
    for p in ds:
        problems.append(f"Found .DS_Store: {p.relative_to(ROOT)}")

    # 4) zip files inside /pages
    pages_dir = ROOT / "pages"
    if pages_dir.exists():
        zips = sorted(p for p in pages_dir.rglob("*.zip") if p.is_file())
        for p in zips:
            problems.append(f"Zip artifact in pages/: {p.relative_to(ROOT)}")

        # 5) python files with spaces in filename inside /pages
        spaced = sorted(p for p in pages_dir.glob("*.py") if " " in p.name)
        for p in spaced:
            problems.append(f"Python page filename contains spaces: {p.relative_to(ROOT)}")

    if problems:
        print("Repo doctor found issues:")
        for i, msg in enumerate(problems, 1):
            print(f"  {i}. {msg}")
        return 1

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

