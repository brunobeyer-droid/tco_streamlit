from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence

import pandas as pd


FetchDf = Callable[[str, Optional[Sequence[Any]]], pd.DataFrame]


@dataclass(frozen=True)
class QualityContext:
    db: Any  # fetch_df callable or object with fetch_df
    year: int
    pi: Optional[int] = None
    programs: tuple[str, ...] = ()
    teams: tuple[str, ...] = ()
    app_groups: tuple[str, ...] = ()
    cost_lines: pd.DataFrame = field(default_factory=pd.DataFrame)
    ado_features: pd.DataFrame = field(default_factory=pd.DataFrame)
    apptio_actuals_breakdown: pd.DataFrame = field(default_factory=pd.DataFrame)
