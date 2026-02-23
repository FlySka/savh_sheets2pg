"""Pipeline común de transformaciones para todas las tablas."""

from __future__ import annotations

from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import step_empty2na, step_normalize_columns_names, step_clean_strings


def get_all_tables_pipeline() -> TablePipeline:
    """Retorna el pipeline común para todas las tablas.

    Returns:
      (TablePipeline): Pipeline con steps comunes a todas las tablas.
    """
    return TablePipeline(
        steps=[
            step_normalize_columns_names(),
            step_empty2na(),
            step_clean_strings(),
        ],
    )
