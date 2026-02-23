import pandas as pd
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_rename_values_to_ids,
    step_add_audit_columns,
    step_parse_dates,
)


def get_pedidos_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename_values_to_ids(
                cols=["cliente", "destinatario", "estado"],                    
                table_name=["clientes", "destinatarios", "dim_estados_pedido"],
                column_lookup=["nombre", "nombre", "estado"],
                tables=tables,
            ),
            step_parse_int(["id", "cliente_id", "destinatario_id", "estado_id"]),
            step_parse_dates(["fecha"]),
            step_add_audit_columns(),
        ],
    )