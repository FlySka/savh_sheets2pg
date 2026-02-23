
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_add_audit_columns,
    step_parse_dates,
)


def get_entregas_pipeline() -> TablePipeline:
    return TablePipeline(
        steps=[
            step_parse_int(["id", "pedido_id"]),
            step_parse_dates(["fecha"]),
            step_add_audit_columns(),
        ],
    )