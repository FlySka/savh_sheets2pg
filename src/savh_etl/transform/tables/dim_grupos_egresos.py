
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_parse_bool,
)


def get_dim_grupos_egresos_pipeline() -> TablePipeline:
    return TablePipeline(
        steps=[
            step_parse_int(["id"]),
            step_parse_bool(["is_active"]),
        ],
    )