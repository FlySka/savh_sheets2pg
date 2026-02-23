import pandas as pd
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_parse_bool,
    step_rename_values_to_ids,
    step_add_audit_columns,
    step_parse_float,
)

def get_compras_producto_items_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename_values_to_ids(
                cols=["producto", "calibre", "variedad", "categoria"],                     # columna en clientes
                table_name=["productos", "dim_calibres", "dim_variedades", "dim_categorias"],            # lookup table
                column_lookup=["nombre", "calibre", "variedad", "categoria"],            # columna en lookup
                tables=tables,
            ),
            step_parse_int(["id", "compra_id", "tipo_id", "producto_id", "calibre_id", "variedad_id", "categoria_id"]),
            step_parse_bool(["is_active", "factura", "factura_despacho"]),
            step_parse_float(["kg", "costo_unit", "costo_unit_con_iva", "costo_total", "costo_total_con_iva"]),
            step_add_audit_columns(),
        ],
    )