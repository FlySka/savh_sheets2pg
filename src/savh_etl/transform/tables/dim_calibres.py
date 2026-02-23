import pandas as pd
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_parse_bool,
    step_rename_values_to_ids,
    step_add_created_at,
    step_add_updated_at,
)

def get_dim_calibres_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename_values_to_ids(
                cols=["producto"],                     # columna en clientes
                table_name=["productos"],            # lookup table
                column_lookup=["nombre"],            # columna en lookup
                tables=tables,
            ),
            step_parse_int(["id", "producto_id"]),
            step_parse_bool(["is_active"]),
        ],
    )