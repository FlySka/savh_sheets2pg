import pandas as pd
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_rename_values_to_ids,
    step_add_audit_columns,
    step_parse_dates,
    step_parse_float,
)


def get_ingresos_producto_items_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename_values_to_ids(
                cols=[
                    "codigo", 
                    "producto", 
                    "variedad", 
                    "categoria",
                    "calibre"
                ],                    
                table_name=[
                    "codigos", 
                    "productos", 
                    "dim_variedades", 
                    "dim_categorias",
                    "dim_calibres"
                ],
                column_lookup=[
                    "codigo", 
                    "nombre",
                    "variedad",
                    "categoria",
                    "calibre"
                ], 
                tables=tables,
            ),
            step_parse_int(["id", "ingreso_id", "codigo_id", "producto_id", "variedad_id", "categoria_id", "calibre_id",]),
            step_parse_float(["kg"]),
            step_add_audit_columns(),
        ],
    )