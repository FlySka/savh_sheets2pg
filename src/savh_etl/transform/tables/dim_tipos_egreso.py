import pandas as pd
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_parse_bool,
    step_rename_values_to_ids,
)

def get_dim_tipos_egreso_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename_values_to_ids(
                cols=["grupo"],                     # columna en clientes
                table_name=["dim_grupos_egresos"],            # lookup table
                column_lookup=["nombre"],            # columna en lookup
                tables=tables,
            ),
            step_parse_int(["id", "grupo_id"]),
            step_parse_bool(["is_active"]),
        ],
    )
