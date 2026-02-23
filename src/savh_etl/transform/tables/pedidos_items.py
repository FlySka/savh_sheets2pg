import pandas as pd
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_rename_values_to_ids,
    step_add_audit_columns,
    step_parse_float,
)


def get_pedidos_items_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename_values_to_ids(
                cols=[
                    "producto", 
                    "variedad", 
                    "categoria", 
                    "calibre", 
                    "codigo"
                ],                   
                table_name=[
                    "productos", 
                    "dim_variedades", 
                    "dim_categorias", 
                    "dim_calibres", 
                    "codigos"
                ],
                column_lookup=[
                    "nombre",  
                    "variedad",
                    "categoria",
                    "calibre", 
                    "codigo"
                ],
                tables=tables,
            ),
            step_parse_int(["id", "pedido_id", "producto_id", "variedad_id", "categoria_id", "calibre_id", "codigo_id",]),
            step_parse_float(["kg", "precio_unit", "precio_total"]),
            step_add_audit_columns(),
        ],
    )