
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_add_audit_columns,
)

def get_pagos_compra_pipeline() -> TablePipeline:
    return TablePipeline(
        steps=[
            step_parse_int(["id", "compra_id", "egreso_id", "monto_aplicado"]),
            step_add_audit_columns(),
        ],
    )