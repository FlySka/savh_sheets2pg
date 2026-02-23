import pandas as pd
from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.steps import (
    step_parse_int,
    step_parse_float,
    step_rename_values_to_ids,
    step_add_audit_columns,
    step_parse_dates,
)


def get_egresos_pipeline(tables: dict[str, pd.DataFrame]) -> TablePipeline:
    return TablePipeline(
        steps=[
            step_rename_values_to_ids(
                cols=[
                    "tipo_economico",
                    "tipo_egreso", 
                    "medio_pago", 
                    "vehiculo", 
                    "trabajador", 
                    "vendedor", 
                    "cliente", 
                    "proveedor"
                ],                    
                table_name=[
                    "dim_tipos_economicos",
                    "dim_tipos_egreso", 
                    "dim_medios_pago", 
                    "vehiculos", 
                    "trabajadores", 
                    "vendedores", 
                    "clientes", 
                    "proveedores"
                ],
                column_lookup=[
                    "tipo",
                    "tipo", 
                    "tipo", 
                    "alias", 
                    "nombre", 
                    "nombre", 
                    "nombre", 
                    "nombre"
                ],
                tables=tables,
            ),
            step_parse_int([
                "id", 
                "tipo_economico_id", 
                "tipo_egreso_id", 
                "medio_pago_id", 
                "vehiculo_id", 
                "trabajador_id", 
                "vendedor_id", 
                "cliente_id", 
                "proveedor_id"
            ]),
            step_parse_dates(["fecha"]),
            step_parse_float(["monto"]),
            step_add_audit_columns(),
        ],
    )