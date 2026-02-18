from __future__ import annotations

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
PAGES_DIR = ROOT / "pages"

# Guardrail intent: when a page performs direct write-like DB operations, it must also
# include a freshness refresh hook in that same page module.
WRITE_PATTERN = re.compile(
    r"\b(execute\(|execute_active\(|upsert_[a-z0-9_]*\(|delete_[a-z0-9_]*\(|sync_[a-z0-9_]*\()",
    re.IGNORECASE,
)
REFRESH_PATTERN = re.compile(r"post_write_refresh\(|_admin_post_write_refresh\(|_settings_post_write_refresh\(")


def test_write_pages_declare_refresh_hooks() -> None:
    failures: list[str] = []
    for page in sorted(PAGES_DIR.glob("*.py")):
        txt = page.read_text(encoding="utf-8")
        if not WRITE_PATTERN.search(txt):
            continue
        if not REFRESH_PATTERN.search(txt):
            failures.append(str(page.relative_to(ROOT)))

    assert not failures, (
        "Write-like page operations must include a colocated refresh hook "
        "(post_write_refresh/_settings_post_write_refresh/_admin_post_write_refresh). "
        f"Missing in: {', '.join(failures)}"
    )
