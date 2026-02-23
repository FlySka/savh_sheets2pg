import pandas as pd
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_rename_values_to_ids,
    step_add_audit_columns,
    step_parse_dates,
)


def get_pagos_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename_values_to_ids(
                cols=["cliente", "medio_pago"],                    
                table_name=["clientes", "dim_medios_pago"],
                column_lookup=["nombre", "tipo"],
                tables=tables,
            ),
            step_parse_int(["id", "cliente_id", "medio_pago_id", "monto"]),
            step_parse_dates(["fecha"]),
            step_add_audit_columns(),
        ],
    )