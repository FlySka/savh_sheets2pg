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
                cols=[
                    "cliente", 
                    "destinatario", 
                    "tipo", 
                    "estado_venta", 
                    "estado_despacho",
                    "estado_facturacion", 
                    "estado_pago"
                ],
                table_name=[
                    "clientes", 
                    "destinatarios",
                    "dim_tipos_venta", 
                    "dim_estados_venta", 
                    "dim_estados_despacho", 
                    "dim_estados_venta_facturacion", 
                    "dim_estados_venta_pago"
                ],
                column_lookup=[
                    "nombre",
                    "nombre",
                    "tipo",
                    "estado",
                    "estado",
                    "estado",
                    "estado"
                ],
                tables=tables,
            ),
            step_parse_int([
                "id", 
                "pedido_id", 
                "cliente_id", 
                "destinatario_id", 
                "tipo_id", 
                "estado_venta_id", 
                "estado_despacho_id", 
                "estado_facturacion_id", 
                "estado_pago_id"
            ]),
            step_parse_dates(["fecha"]),
            step_add_audit_columns(),
        ],
    )