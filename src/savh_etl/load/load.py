# load/load.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from savh_etl.io.pg_client import PostgresClient
from savh_etl.load.ddl import get_table_columns_meta, run_sql_files, sync_identity_sequences
from savh_etl.load.load_strategy import build_savh_load_plan
from savh_etl.settings import LoadMode as SchemaLoadMode
from savh_etl.utils.logging import get_logger

log = get_logger(__name__)


DEFAULT_PRE_DDL_FILES = (
    "01_schema.sql",
    "02_tables.sql",
    "03_constraints.sql",
)

DEFAULT_POST_DDL_FILES = (
    "04_foreign_keys.sql",
    "05_indexes.sql",
    "06_comments.sql",
    "07_triggers.sql",
)


def _default_sql_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "sql"


def _normalize_nulls(df: pd.DataFrame) -> pd.DataFrame:
    # NaN/NaT -> None (mejor para insertar)
    return df.where(pd.notnull(df), None)


def _schema_for_table(
    table: str,
    *,
    core_schema: str,
    ingest_schema: str,
    audit_schema: str,
) -> str:
    if table in {"ingest_events", "entity_events"}:
        return ingest_schema
    if table == "audit_log":
        return audit_schema
    return core_schema


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _truncate_all_tables(engine: Engine, schemas: tuple[str, ...]) -> int:
    """Trunca todas las tablas base dentro de `schemas` (RESTART IDENTITY CASCADE)."""
    if not schemas:
        return 0

    with engine.begin() as conn:
        tables: list[tuple[str, str]] = []
        for schema in schemas:
            rows = conn.execute(
                text(
                    """
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema = :schema
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                    """
                ),
                {"schema": schema},
            ).all()
            tables.extend([(r[0], r[1]) for r in rows])

        if not tables:
            return 0

        qualified = ", ".join(f"{_quote_ident(s)}.{_quote_ident(t)}" for s, t in tables)
        conn.exec_driver_sql(f"TRUNCATE TABLE {qualified} RESTART IDENTITY CASCADE;")
        return len(tables)


def _validate_and_align_df(
    engine: Engine,
    df: pd.DataFrame,
    *,
    schema: str,
    table: str,
    drop_extra_columns: bool,
) -> pd.DataFrame:
    meta = get_table_columns_meta(engine, schema, table)
    if not meta:
        raise ValueError(
            f"La tabla '{schema}.{table}' no existe o no tiene columnas (¿DDL corrió?)."
        )

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    db_cols = [c.name for c in meta]
    df_cols = list(df.columns)

    required = {
        c.name
        for c in meta
        if (not c.is_nullable) and (not c.has_default) and (not c.is_identity)
    }
    missing_required = required - set(df_cols)
    if missing_required:
        raise ValueError(
            f"En '{schema}.{table}' faltan columnas requeridas en el DF: {sorted(missing_required)}"
        )

    extra = set(df_cols) - set(db_cols)
    if extra:
        if not drop_extra_columns:
            raise ValueError(
                f"En '{schema}.{table}' sobran columnas no existentes en DB: {sorted(extra)}"
            )
        log.warning("Drop columnas extra en %s.%s: %s", schema, table, sorted(extra))
        df = df[[c for c in df_cols if c in set(db_cols)]]

    ordered = [c for c in db_cols if c in df.columns]
    return df[ordered]


@dataclass(frozen=True)
class LoadResult:
    tables_loaded: int
    rows_loaded: int
    rows_by_table: dict[str, int]


def load(
    dict_df: Mapping[str, pd.DataFrame],
    *,
    dsn: Optional[str] = None,
    sql_dir: str | Path | None = None,
    core_schema: str = "core",
    ingest_schema: str = "ingest",
    audit_schema: str = "audit",
    load_mode: SchemaLoadMode = SchemaLoadMode.drop_create,
    pg_chunksize: int = 5000,
    ddl_only: bool = False,
    drop_extra_columns: bool = True,
    echo_sql: bool = False,
    analyze: bool = False,
) -> LoadResult:
    """Carga one-shot (dict tabla -> DataFrame) a PostgreSQL usando DDL explícito.

    Flujo:
      1) (drop_create) drop schemas core/ingest/audit
         (truncate) TRUNCATE de tablas existentes (si el schema ya está inicializado)
      2) DDL pre-load: 01_schema + 02_tables + 03_constraints
         - si `ddl_only=True`, termina aquí (sin insertar data)
      3) Insert (append) según `build_savh_load_plan()` + fallback a tablas extra existentes
      4) Sync secuencias identity/serial (columna 'id')
      5) DDL post-load: 05_indexes + 04_foreign_keys + 06_comments + 07_triggers
    """
    if dsn is None:
        dsn = (
            os.getenv("SAVH_ETL_PG_DSN")
            or os.getenv("DATABASE_URL")
            or os.getenv("POSTGRES_DSN")
        )
    if not dsn:
        raise ValueError("Falta DSN. Pasa dsn=... o define SAVH_ETL_PG_DSN/DATABASE_URL.")

    sql_dir_path = Path(sql_dir) if sql_dir is not None else _default_sql_dir()

    ddl_needed = load_mode == SchemaLoadMode.drop_create

    with PostgresClient(dsn) as pg:
        engine = pg.engine()
        if echo_sql:
            engine.echo = True  # type: ignore[attr-defined]

        # * 1) Reset
        schemas = (core_schema, ingest_schema, audit_schema)
        if load_mode == SchemaLoadMode.drop_create:
            for s in schemas:
                log.info("Reset DB: drop schema %s", s)
                pg.drop_schema(s)
            ddl_needed = True

        elif load_mode == SchemaLoadMode.truncate:
            for s in schemas:
                pg.create_schema(s)

            truncated = _truncate_all_tables(engine, schemas)
            if truncated == 0:
                # DB/schema vacío => corremos DDL completo (como bootstrap)
                log.info("No hay tablas para truncar; asumo DB sin inicializar y corro DDL.")
                ddl_needed = True
            else:
                log.info("TRUNCATE OK | tablas=%d", truncated)
                ddl_needed = False
        else:
            raise ValueError(f"load_mode no soportado: {load_mode}")

        # * 2) DDL pre/post
        if ddl_needed:
            run_sql_files(engine, sql_dir_path, DEFAULT_PRE_DDL_FILES)

        if ddl_only:
            log.info("DDL-only: se omite inserción y post-DDL.")
            return LoadResult(tables_loaded=0, rows_loaded=0, rows_by_table={})

        # * 3) Load (ordenado por plan)
        plan = build_savh_load_plan()
        loaded_tables: list[tuple[str, str]] = []  # (schema, table)
        loaded_keys: set[str] = set()
        rows_by_table: dict[str, int] = {}

        def _load_one(*, table: str, df_key: str) -> None:
            nonlocal rows_by_table
            if df_key not in dict_df:
                return

            df = dict_df[df_key]
            if df is None or not isinstance(df, pd.DataFrame):
                raise TypeError(f"dict_df['{df_key}'] no es DataFrame.")
            if df.empty:
                return

            schema = _schema_for_table(
                table,
                core_schema=core_schema,
                ingest_schema=ingest_schema,
                audit_schema=audit_schema,
            )
            if not pg.table_exists(schema, table):
                log.warning("Tabla no existe, se omite: %s.%s (df_key=%s)", schema, table, df_key)
                return

            df2 = _normalize_nulls(df)
            df2 = _validate_and_align_df(
                engine,
                df2,
                schema=schema,
                table=table,
                drop_extra_columns=drop_extra_columns,
            )

            log.info("Cargando: %s.%s | filas=%d cols=%d", schema, table, len(df2), df2.shape[1])
            pg.write_dataframe(
                df=df2,
                table_name=table,
                schema=schema,
                if_exists="append",
                chunksize=pg_chunksize,
                index=False,
            )
            loaded_tables.append((schema, table))
            loaded_keys.add(df_key)
            rows_by_table[f"{schema}.{table}"] = int(len(df2))

        for spec in plan.order:
            _load_one(table=spec.name, df_key=spec.key())

        # fallback: si hay dataframes extra que coinciden con tablas existentes en core
        for df_key in dict_df.keys():
            if df_key in loaded_keys:
                continue
            table = str(df_key).strip()
            if not table:
                continue
            if "." in table:
                # soporta "schema.tabla" si aparece
                sch, tbl = table.split(".", 1)
                if sch and tbl and pg.table_exists(sch, tbl):
                    _load_one(table=tbl, df_key=df_key)
                continue

            if pg.table_exists(core_schema, table):
                _load_one(table=table, df_key=df_key)

        # * 4) Sync sequences (solo id)
        for schema, table in loaded_tables:
            try:
                sync_identity_sequences(engine, schema, table, id_column="id")
            except Exception:
                log.warning("No se pudo sync secuencia para %s.%s (¿tiene columna 'id'?)", schema, table)
                continue

        # * 5) Post DDL (solo cuando bootstrapeamos DDL)
        if ddl_needed:
            run_sql_files(engine, sql_dir_path, DEFAULT_POST_DDL_FILES)

        if analyze:
            with engine.begin() as conn:
                conn.exec_driver_sql("ANALYZE;")

        rows_loaded = int(sum(rows_by_table.values()))
        return LoadResult(
            tables_loaded=len(rows_by_table),
            rows_loaded=rows_loaded,
            rows_by_table=rows_by_table,
        )
