from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Optional
import inspect
import pandas as pd

TransformFn = Callable[..., pd.DataFrame]


@dataclass(frozen=True)
class TransformContext:
    tables: Dict[str, pd.DataFrame]


@dataclass(frozen=True)
class TablePipeline:
    """Pipeline de transformaciones para una tabla.

    Attributes:
        steps: Lista de funciones que transforman un DataFrame.
    """

    steps: list[TransformFn]


def apply_pipeline(df: pd.DataFrame, pipeline, ctx: Optional[TransformContext] = None) -> pd.DataFrame:
    """Aplica steps en orden.
    - steps antiguos: fn(df) -> df
    - steps nuevos: fn(df, ctx) -> df
    """
    out = df
    for step in pipeline.steps:
        n_params = len(inspect.signature(step).parameters)
        if ctx is not None and n_params >= 2:
            out = step(out, ctx)
        else:
            out = step(out)
    return out
