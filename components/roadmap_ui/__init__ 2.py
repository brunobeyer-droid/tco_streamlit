import os
from pathlib import Path

import streamlit.components.v1 as components

_COMPONENT_DIR = Path(__file__).parent.resolve()
_FRONTEND_DIST = (_COMPONENT_DIR / "frontend" / "dist").resolve()

_DEV_URL = os.getenv("ROADMAP_UI_DEV_URL", "").strip()

if _DEV_URL:
    _component = components.declare_component("roadmap_ui", url=_DEV_URL)
else:
    _component = components.declare_component("roadmap_ui", path=str(_FRONTEND_DIST))


def roadmap_ui(data: dict, height: int = 850, key: str = "roadmap_ui"):
    return _component(data=data, height=height, key=key)


def roadmap_ui_debug_info() -> dict:
    index_html = _FRONTEND_DIST / "index.html"
    assets_dir = _FRONTEND_DIST / "assets"
    dist_listing = []
    assets_listing = []
    try:
        if _FRONTEND_DIST.exists():
            dist_listing = sorted([p.name for p in _FRONTEND_DIST.iterdir()])
    except Exception:
        dist_listing = []
    try:
        if assets_dir.exists():
            assets_listing = sorted([p.name for p in assets_dir.iterdir()])
    except Exception:
        assets_listing = []
    return {
        "component_dir": str(_COMPONENT_DIR),
        "frontend_dist": str(_FRONTEND_DIST),
        "index_exists": index_html.exists(),
        "dev_url": _DEV_URL,
        "dist_listing": dist_listing,
        "assets_listing": assets_listing,
    }


__all__ = ["roadmap_ui", "roadmap_ui_debug_info"]
