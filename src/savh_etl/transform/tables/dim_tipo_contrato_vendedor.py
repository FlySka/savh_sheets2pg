from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_bool,
    step_parse_int,
)


def get_dim_tipo_contrato_vendedor_pipeline() -> TablePipeline:
    return TablePipeline(
        steps=[
            step_parse_int(["id"]),
            step_parse_bool(["is_active"]),
        ],
    )
