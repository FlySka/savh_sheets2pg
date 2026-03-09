import pandas as pd

from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_add_created_at,
    step_add_updated_at,
    step_parse_bool,
    step_parse_float,
    step_parse_int,
    step_rename,
    step_rename_values_to_ids,
)


def get_contratos_vendedor_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename({"column_7": "vendedor"}),
            step_rename_values_to_ids(
                cols=["vendedor", "tipo", "base"],
                table_name=[
                    "vendedores",
                    "dim_tipo_contrato_vendedor",
                    "dim_base_comision_vendedor",
                ],
                column_lookup=["nombre", "tipo", "base"],
                tables=tables,
            ),
            step_parse_int(["id", "vendedor_id", "tipo_id", "base_id"]),
            step_parse_float(["tasa_comision"]),
            step_parse_bool(["is_active"]),
            step_add_created_at(),
            step_add_updated_at(),
        ],
    )
