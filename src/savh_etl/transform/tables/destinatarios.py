import pandas as pd
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_parse_bool,
    step_rename_values_to_ids,
    step_add_created_at,
    step_add_updated_at,
)


def get_destinatarios_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename_values_to_ids(
                cols=["cliente"],
                table_name=["clientes"],
                column_lookup=["nombre"],
                tables=tables,
            ),
            step_parse_int(["id", "cliente_id"]),
            step_parse_bool(["is_active"]),
            step_add_created_at(),
            step_add_updated_at(),
        ],
    )
