from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_bool,
    step_parse_int,
    step_rename,
)


def get_dim_tipos_cliente_pipeline() -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename({"nombre": "tipo"}),
            step_parse_int(["id"]),
            step_parse_bool(["is_active"]),
        ],
    )
