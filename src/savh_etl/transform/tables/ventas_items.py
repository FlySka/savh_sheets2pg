import pandas as pd
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_parse_bool,
    step_rename_values_to_ids,
    step_add_audit_columns,
    step_parse_float,
)


def get_ventas_items_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename_values_to_ids(
                cols=[
                    "producto", 
                    "calibre", 
                    "variedad", 
                    "categoria", 
                    "codigo"
                ],                     
                table_name=[
                    "productos", 
                    "dim_calibres", 
                    "dim_variedades", 
                    "dim_categorias", 
                    "codigos"
                ],
                column_lookup=[
                    "nombre", 
                    "calibre", 
                    "variedad",
                    "categoria",
                    "codigo"
                ], 
                tables=tables,
            ),
            step_parse_int(["id", "venta_id", "producto_id", "calibre_id", "variedad_id", "categoria_id", "codigo_id"]),
            step_parse_bool(["is_active", "factura", "factura_despacho"]),
            step_parse_float(["kg", "precio_unit", "precio_total"]), 
            step_add_audit_columns(),
        ],
    )