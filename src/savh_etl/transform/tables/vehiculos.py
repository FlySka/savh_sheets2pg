
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_parse_bool,
    step_add_created_at,
    step_add_updated_at,
)


def get_vehiculos_pipeline() -> TablePipeline:
    return TablePipeline(
        steps=[
            step_parse_int(["id", "anio"]),
            step_parse_bool(["is_active"]),
            step_add_created_at(),
            step_add_updated_at(),
        ],
    )
