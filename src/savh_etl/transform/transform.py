"""Utilidades de transformación de datos.
Objetivo:
- Proveer funciones y clases para transformar DataFrames pandas.
Estas transformaciones pueden incluir limpieza, tipado, renombrado de columnas, etc.
"""

from __future__ import annotations

import pandas as pd
from copy import deepcopy

from savh_etl.transform.pipeline import apply_pipeline, TransformContext
from savh_etl.transform.registry import get_registry
from savh_etl.transform.mapping import mapping_terceros_ids, normalize_tables_names
from savh_etl.transform.build import build_terceros_tables
from savh_etl.transform.tables.all_tables import get_all_tables_pipeline
from savh_etl.utils.logging import get_logger
log = get_logger(__name__)

def transform_all_tables(dict_df: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    dict_df = normalize_tables_names(dict_df)

    registry = get_registry(deepcopy(dict_df))
    all_tables_pipeline = get_all_tables_pipeline()

    # 1) PASS 1: limpieza común para TODAS (stage1)
    stage1: dict[str, pd.DataFrame] = {}
    for table_name, table_df in dict_df.items():
        log.info("Limpieza base: %s | filas=%d", table_name, len(table_df))
        stage1[table_name] = apply_pipeline(table_df, all_tables_pipeline)

    # Contexto con lookups ya limpios
    ctx = TransformContext(tables=stage1)

    # 2) PASS 2: pipelines por tabla (ya puedes hacer rename_values_to_ids)
    out: dict[str, pd.DataFrame] = {}
    for table_name, df in stage1.items():
        log.info("Transform específico: %s | filas=%d", table_name, len(df))
        table_pipeline = registry.get(table_name)
        if table_pipeline is not None:
            out[table_name] = apply_pipeline(df, table_pipeline, ctx=ctx)
        else:
            out[table_name] = df

    return out

def transform(dict_df: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transforma una tabla específica dentro del diccionario de DataFrames.

    Args:
        dict_df (dict[str, pd.DataFrame]): Diccionario de DataFrames.

    Returns:
        dict[str, pd.DataFrame]: Diccionario con la tablas transformadas.
    """
    all_tables = transform_all_tables(dict_df)
    tercero_tables = build_terceros_tables(all_tables)
    all_tables = mapping_terceros_ids(all_tables, tercero_tables)
    all_tables.update(tercero_tables)
    return all_tables
