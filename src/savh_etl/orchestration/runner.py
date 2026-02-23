"""Orquestación del pipeline para el ETL one-shot.

Este módulo coordina el proceso de punta a punta:
  1) Extraer todas las hojas del origen como DataFrames.
  2) Resetear el schema destino según `load_mode`.
  3) Cargar cada DataFrame a PostgreSQL.

Nota:
  La carga usa DDL explícito (archivos `src/savh_etl/sql/*.sql`) + inserciones
  con `DataFrame.to_sql(if_exists="append")`. Las FK/indexes/triggers se aplican
  según el orden definido en `savh_etl.load.load`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

import pandas as pd
from savh_etl.utils.dataframe import dataframe_schema_report

from savh_etl.extract.extract import extract
from savh_etl.transform.transform import transform
from savh_etl.load.load import load as load_to_postgres
from savh_etl.settings import ETLSettings, LoadMode
from savh_etl.utils.logging import get_logger
log = get_logger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    """Resultado agregado de una ejecución del pipeline.

    Attributes:
      tablas_extraidas: Cantidad de tablas extraídas.
      filas_extraidas_total: Total de filas extraídas.
      tablas_cargadas: Cantidad de tablas cargadas.
      filas_cargadas_total: Total de filas cargadas.
    """

    tablas_extraidas: int
    filas_extraidas_total: int
    tablas_cargadas: int
    filas_cargadas_total: int


def run_pipeline(settings: ETLSettings) -> PipelineResult:
    """Ejecuta el pipeline end-to-end.

    Args:
      settings: Configuración de ejecución (source, pg_dsn, schema, etc.).

    Returns:
      PipelineResult con totales de extracción y carga.

    Raises:
      ValueError: Si falta `pg_dsn` cuando corresponde cargar.
    """
    log.info(
        "Inicio ETL | core_schema=%s | load_mode=%s | dry_run=%s | only_ddl=%s",
        settings.pg_schema_core,
        settings.load_mode,
        settings.dry_run,
        settings.ddl_only,
    )

    # * 1) Extracción
    tablas: Dict[str, pd.DataFrame] = extract(settings)
    filas_extraidas = int(sum(len(df) for df in tablas.values()))
    log.info(
        "Extracción OK | tablas=%d | filas_total=%d",
        len(tablas),
        filas_extraidas,
    )
    log.debug("Tablas extraídas: %s", ", ".join(tablas.keys()))
    if settings.debug_table_extract:
        if settings.debug_table_extract in tablas:
            log.debug(f"Tabla de debug '%s':", settings.debug_table_extract)
            log.debug(
                "Schema tabla '%s':\n%s", 
                settings.debug_table_extract, 
                dataframe_schema_report(tablas[settings.debug_table_extract])
            )
            log.debug(tablas[settings.debug_table_extract].head(3).to_string())
        else:
            log.warning(
                "Tabla de debug '%s' no encontrada entre las tablas extraídas.",
                settings.debug_table_extract,
            )

    # * 2) Transformacion
    tablas = transform(tablas)
    log.info("Transformación OK | tablas=%d", len(tablas))
    log.debug("Tablas transformadas: %s", ", ".join(tablas.keys()))
    if settings.debug_table_transform:
        if settings.debug_table_transform in tablas:
            log.debug(f"Tabla de debug '%s':", 
            settings.debug_table_transform)
            log.debug(
                "Schema tabla '%s':\n%s", 
                settings.debug_table_transform, 
                dataframe_schema_report(tablas[settings.debug_table_transform])
            )
            log.debug(tablas[settings.debug_table_transform].head(78).to_string())
        else:
            log.warning(
                "Tabla de debug '%s' no encontrada entre las tablas transformadas.",
                settings.debug_table_transform,
            )

    #  Salida temprana (solo extract + transform)
    if settings.dry_run:
        if settings.ddl_only:
            log.warning(
                "ddl_only=True pero dry_run=True: no se ejecuta DDL. Usa --no-dry-run para correr solo DDL."
            )
        log.info("Se omite carga (dry_run=%s).", settings.dry_run)
        return PipelineResult(
            tablas_extraidas=len(tablas),
            filas_extraidas_total=filas_extraidas,
            tablas_cargadas=0,
            filas_cargadas_total=0,
        )

    if not settings.pg_dsn:
        raise ValueError(
            "pg_dsn está vacío. Define SAVH_ETL_PG_DSN o pásalo por --pg-dsn."
        )

    # * 3) Carga a PostgreSQL
    result = load_to_postgres(
        tablas,
        dsn=settings.pg_dsn,
        core_schema=settings.pg_schema_core,
        ingest_schema=settings.pg_schema_ingest,
        audit_schema=settings.pg_schema_audit,
        load_mode=settings.load_mode,
        pg_chunksize=settings.pg_chunksize,
        ddl_only=settings.ddl_only,
    )

    log.info(
        "Carga OK | tablas=%d | filas_total=%d",
        result.tables_loaded,
        result.rows_loaded,
    )

    return PipelineResult(
        tablas_extraidas=len(tablas),
        filas_extraidas_total=filas_extraidas,
        tablas_cargadas=result.tables_loaded,
        filas_cargadas_total=result.rows_loaded,
    )
