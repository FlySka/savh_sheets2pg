import pandas as pd
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_rename_values_to_ids,
    step_add_audit_columns,
    step_parse_dates,
)

def get_compras_producto_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename_values_to_ids(
                cols=["proveedor", "estado_compra"],                     # columna en clientes
                table_name=["proveedores", "dim_estados_compra"],            # lookup table
                column_lookup=["nombre", "estado"],            # columna en lookup
                tables=tables,
            ),
            step_parse_int(["id", "proveedor_id", "estado_compra_id"]),
            step_parse_dates(["fecha"]),
            step_add_audit_columns(),
        ],
    )