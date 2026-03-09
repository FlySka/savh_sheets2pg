"""
Módulo de transformaciones específicas para la tabla 'clientes'.
Aquí se definen las transformaciones particulares que se aplican
a los datos de clientes durante el proceso ETL.
"""
import pandas as pd
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_parse_bool,
    step_rename_values_to_ids,
    step_add_created_at,
    step_add_updated_at,
)

def get_clientes_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename_values_to_ids(
                cols=["tipo", "vendedor"],
                table_name=["dim_tipos_cliente", "vendedores"],
                column_lookup=["nombre", "nombre"],
                tables=tables,
            ),
            step_parse_int(["id", "tipo_id", "vendedor_id"]),
            step_parse_bool(["is_active", "factura", "factura_despacho"]),
            step_add_created_at(),
            step_add_updated_at(),
        ],
    )
