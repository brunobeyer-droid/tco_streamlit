from __future__ import annotations
from typing import Dict, Callable
import pandas as pd
from viz.charts import opt_bar_top_spend_by, opt_stacked_by_category, opt_pie_share_by

# “Registry” of visuals so your dashboard can import by name/ID if you’d like.
# Each entry maps to a function that returns an ECharts option dict.

REGISTRY: Dict[str, Callable[..., dict]] = {
    "top_spend_by_program": opt_bar_top_spend_by,
    "stacked_spend_by_category": opt_stacked_by_category,
    "share_by_source": opt_pie_share_by,
}

def get_visual(name: str):
    return REGISTRY.get(name)
