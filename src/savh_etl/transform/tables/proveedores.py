"""
Módulo de transformaciones específicas para la tabla 'proveedores'.
Aquí se definen las transformaciones particulares que se aplican
a los datos de proveedores durante el proceso ETL.
"""

from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_parse_bool,
    step_add_created_at,
    step_add_updated_at,
)

def get_proveedores_pipeline() -> TablePipeline:
    return TablePipeline(
        steps=[
            step_parse_int(["id"]),
            step_parse_bool(["is_active"]),
            step_add_created_at(),
            step_add_updated_at(),
        ],
    )

