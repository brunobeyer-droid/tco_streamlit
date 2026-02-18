from __future__ import annotations

import subprocess
from typing import Optional

import streamlit as st


def dot_available() -> bool:
    try:
        result = subprocess.run(["dot", "-V"], capture_output=True, text=True, check=False)
        return result.returncode == 0
    except Exception:
        return False


def render_dot_to_svg(dot_str: str) -> Optional[bytes]:
    try:
        result = subprocess.run(
            ["dot", "-Tsvg"],
            input=dot_str.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if result.returncode != 0:
            return None
        return result.stdout
    except Exception:
        return None


def render_svg_scrollable(svg_bytes: bytes, *, height_px: int = 800, border: bool = True) -> None:
    border_css = "border:1px solid rgba(148,163,184,0.4);" if border else ""
    svg_text = svg_bytes.decode("utf-8", errors="ignore")
    st.components.v1.html(
        f"""
        <div style="overflow:auto; width:100%; height:{height_px}px; {border_css} border-radius:12px; padding:8px;">
          {svg_text}
        </div>
        """,
        height=height_px + 40,
    )


def download_svg_button(svg_bytes: bytes, filename: str) -> None:
    st.download_button(
        "Download SVG",
        data=svg_bytes,
        file_name=filename,
        mime="image/svg+xml",
    )
