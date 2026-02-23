"""
Módulo de transformaciones específicas para la tabla 'vendedores'.
Aquí se definen las transformaciones particulares que se aplican
a los datos de vendedores durante el proceso ETL.
"""
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_parse_bool,
    step_parse_float,
    step_add_created_at,
    step_add_updated_at,
)

def get_vendedores_pipeline() -> TablePipeline:
    return TablePipeline(
        steps=[
            step_parse_int(["id", "modelo_comercial"]),
            step_parse_float(["comision", "modelo_comercial"]),
            step_parse_bool(["is_active", "tipo"]),
            step_parse_float(["comision"]),
            step_add_created_at(),
            step_add_updated_at(),
        ],
    )
