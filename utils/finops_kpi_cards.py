from __future__ import annotations

import html
from typing import Optional, Sequence


_STATUS_META: dict[str, tuple[str, str]] = {
    "good": ("check_circle", "Good"),
    "watch": ("schedule", "Watch"),
    "risk": ("warning", "Risk"),
    "neutral": ("help", "Info"),
}

_TREND_ICON_META: dict[str, str] = {
    "good": "trending_up",
    "watch": "trending_flat",
    "risk": "trending_down",
    "neutral": "trending_flat",
}


def build_finops_kpi_card_html(
    title: str,
    value: str,
    *,
    tip: str = "",
    chips: Optional[Sequence[str]] = None,
    lines: Optional[Sequence[str]] = None,
    accent_color: str = "#64748b",
    extra_class: str = "",
    icon: str = "insights",
    status: str = "neutral",
    status_label: str = "",
    show_status: bool = False,
    trend_text: str = "",
    trend_tone: str = "neutral",
    trend_icon: str = "",
    spark_values: Optional[Sequence[float]] = None,
    spark_tone: str = "neutral",
    spark_motif: str = "bars",
    extra_lines: int = 3,
) -> str:
    safe_title = html.escape(str(title or ""))
    safe_value = html.escape(str(value or ""))
    safe_tip = html.escape(str(tip or ""))
    safe_icon = html.escape(str(icon or "insights"))

    status_key = str(status or "neutral").strip().lower()
    if status_key not in _STATUS_META:
        status_key = "neutral"
    status_icon, status_default_label = _STATUS_META[status_key]
    safe_status_label = html.escape(str(status_label or status_default_label))

    trend_key = str(trend_tone or "neutral").strip().lower()
    if trend_key not in _TREND_ICON_META:
        trend_key = "neutral"
    trend_icon_name = str(trend_icon or _TREND_ICON_META[trend_key]).strip() or _TREND_ICON_META[trend_key]
    safe_trend_icon = html.escape(trend_icon_name)
    safe_trend_text = html.escape(str(trend_text or "").strip())
    trend_html = ""
    if safe_trend_text:
        trend_html = (
            f"<span class='finops-kpi-trend finops-kpi-trend--{html.escape(trend_key)}'>"
            f"<span class='material-symbols-outlined'>{safe_trend_icon}</span>"
            f"{safe_trend_text}"
            "</span>"
        )

    spark_key = str(spark_tone or "neutral").strip().lower()
    if spark_key not in _TREND_ICON_META:
        spark_key = "neutral"
    motif_key = str(spark_motif or "bars").strip().lower()
    if motif_key not in {"bars", "wave", "dotline", "curve"}:
        motif_key = "bars"

    bg_art_html = ""
    if spark_values:
        vals = [float(v) for v in spark_values if v is not None]
        if vals:
            vmin = min(vals)
            vmax = max(vals)
            span = (vmax - vmin) if (vmax - vmin) > 1e-9 else 1.0
            norm_vals: list[float] = []
            for raw in vals[:10]:
                norm = (raw - vmin) / span
                norm_vals.append(max(0.0, min(1.0, norm)))
            motif_html = ""
            dot_overlay_html = ""
            n = max(2, len(norm_vals))
            curve_x_min = 0.4
            curve_x_max = 99.6
            points_line: list[str] = []
            points_fill: list[str] = ["0,100"]
            dot_html: list[str] = []
            points_xy: list[tuple[float, float]] = []
            for i, norm in enumerate(norm_vals):
                if motif_key == "curve":
                    # Keep a tiny inset to avoid cap clipping while still touching card edges visually.
                    x = (curve_x_min + (i * (curve_x_max - curve_x_min)) / (n - 1)) if n > 1 else 50.0
                else:
                    x = (i * 100.0) / (n - 1) if n > 1 else 50.0
                y = 84.0 - (norm * 62.0)
                points_xy.append((x, y))
                points_line.append(f"{x:.1f},{y:.1f}")
                points_fill.append(f"{x:.1f},{y:.1f}")
                if motif_key == "dotline":
                    dot_html.append(
                        f"<span class='finops-kpi-bg-dot-el' style='left:{x:.1f}%; top:{y:.1f}%;'></span>"
                    )
            points_fill.append("100,100")
            curve_path = ""
            curve_fill_path = ""
            if motif_key == "curve" and len(points_xy) >= 2:
                p = points_xy
                d = [f"M {p[0][0]:.2f} {p[0][1]:.2f}"]
                if len(p) == 2:
                    d.append(f"L {p[1][0]:.2f} {p[1][1]:.2f}")
                else:
                    for i in range(1, len(p) - 1):
                        xc = (p[i][0] + p[i + 1][0]) / 2.0
                        yc = (p[i][1] + p[i + 1][1]) / 2.0
                        d.append(f"Q {p[i][0]:.2f} {p[i][1]:.2f} {xc:.2f} {yc:.2f}")
                    d.append(
                        f"Q {p[-1][0]:.2f} {p[-1][1]:.2f} {p[-1][0]:.2f} {p[-1][1]:.2f}"
                    )
                curve_path = " ".join(d)
                curve_fill_path = (
                    f"{curve_path} L {curve_x_max:.2f} 100.00 L {curve_x_min:.2f} 100.00 Z"
                )
            motif_html = (
                "<svg class='finops-kpi-bg-svg' viewBox='0 0 100 100' preserveAspectRatio='none'>"
                + (
                    f"<path class='finops-kpi-bg-fill' d=\"{curve_fill_path}\" />"
                    if motif_key == "curve" and curve_fill_path
                    else f"<polygon class='finops-kpi-bg-fill' points=\"{' '.join(points_fill)}\" />"
                )
                + (
                    f"<path class='finops-kpi-bg-curve' d=\"{curve_path}\" />"
                    if motif_key == "curve" and curve_path
                    else f"<polyline class='finops-kpi-bg-poly' points=\"{' '.join(points_line)}\" />"
                )
                + "</svg>"
            )
            if dot_html:
                dot_overlay_html = "<div class='finops-kpi-bg-dots'>" + "".join(dot_html) + "</div>"

            bg_art_html = (
                f"<div class='finops-kpi-bg-art finops-kpi-bg-art--{html.escape(spark_key)} finops-kpi-bg-art--{html.escape(motif_key)}' aria-hidden='true'>"
                + motif_html
                + dot_overlay_html
                + "</div>"
            )

    rows = [str(x) for x in (lines or [])]
    while len(rows) < int(extra_lines):
        rows.append("")
    rows = rows[: int(extra_lines)]
    chips_html = "".join(
        f"<span class='finops-kpi-chip'>{html.escape(str(label))}</span>" for label in (chips or [])
    )
    lines_html = "".join(
        f"<div class='finops-kpi-line'>{html.escape(str(line)) if str(line).strip() else '&nbsp;'}</div>"
        for line in rows
    )
    status_html = ""
    if bool(show_status):
        status_html = (
            f"<span class=\"finops-kpi-status finops-kpi-status--{html.escape(status_key)}\">"
            f"<span class=\"material-symbols-outlined\">{html.escape(status_icon)}</span>{safe_status_label}"
            "</span>"
        )

    return "\n".join(
        [
            f"<div class=\"finops-kpi-card {html.escape(extra_class)}\" style=\"--finops-kpi-accent:{accent_color};\">",
            f"<div class=\"finops-kpi-accent\" style=\"background:{accent_color};\"></div>",
            "<div class=\"finops-kpi-bg-layer\" aria-hidden=\"true\"></div>",
            bg_art_html,
            "<div class=\"finops-kpi-watermark\" aria-hidden=\"true\">",
            f"<span class=\"material-symbols-outlined\">{safe_icon}</span>",
            "</div>",
            "<div class=\"finops-kpi-body\">",
            "<div>",
            "<div class=\"finops-kpi-header\">",
            "<div class=\"finops-kpi-title-wrap\">",
            f"<span class=\"material-symbols-outlined finops-kpi-title-icon\">{safe_icon}</span>",
            f"<div class=\"finops-kpi-title\">{safe_title}</div>",
            "</div>",
            "<div class=\"finops-kpi-meta\">",
            status_html,
            f"<div class=\"finops-kpi-help\" title=\"{safe_tip}\">?</div>",
            "</div>",
            "</div>",
            "<div class=\"finops-kpi-value-row\">",
            f"<div class=\"finops-kpi-value\">{safe_value}</div>",
            trend_html,
            "</div>",
            f"<div class=\"finops-kpi-chips\">{chips_html}</div>",
            "</div>",
            f"<div class=\"finops-kpi-lines\">{lines_html}</div>",
            "</div>",
            "</div>",
        ]
    )
