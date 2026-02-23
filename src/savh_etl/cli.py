"""CLI para el ETL de SAVH (one-shot).

Este módulo define la interfaz de línea de comandos para ejecutar un ETL
de una sola pasada desde un origen (Excel o Google Sheets) hacia PostgreSQL.

Comandos:
  - run: ejecuta el pipeline (extract + opcional load).
  - config: imprime la configuración efectiva.
  - list: lista las hojas disponibles en el origen.

Ejemplos:
  poetry run savh-etl run --source excel --excel-path /mnt/data/BD_SAVH.xlsx --pg-dsn ...
  poetry run savh-etl list --source excel --excel-path /mnt/data/BD_SAVH.xlsx
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import typer

from savh_etl.orchestration.runner import run_pipeline
from savh_etl.settings import ETLSettings, LoadMode

from savh_etl.logging_config import setup_logging
from savh_etl.settings import ETLSettings

settings = ETLSettings()
setup_logging(settings.log_level)

log = logging.getLogger(__name__)

app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    help="ETL one-shot: Excel/Google Sheets ➜ PostgreSQL",
)


@app.command("run")
def run(
    excel_path: Optional[Path] = typer.Option(
        None,
        "--excel-path",
        dir_okay=False,
        help="Ruta al archivo .xlsx (si source=excel).",
    ),
    pg_dsn: Optional[str] = typer.Option(
        None,
        "--pg-dsn",
        help=(
            "DSN de Postgres (SQLAlchemy). Ej: "
            "postgresql+psycopg://user:pass@host:5432/db"
        ),
    ),
    schema: Optional[str] = typer.Option(
        None,
        "--schema",
        help="Schema core destino (por defecto: core).",
    ),
    load_mode: Optional[LoadMode] = typer.Option(
        None,
        "--load-mode",
        help="Estrategia de reset: drop_create o truncate.",
    ),
    exclude_sheets: Optional[str] = typer.Option(
        None,
        "--exclude",
        help='JSON array con hojas a excluir. Ej: \'["_tmp","README"]\'.',
    ),
    dry_run: Optional[bool] = typer.Option(
        False,
        "--dry-run",
        help="Si dry-run, no escribe en Postgres; solo extrae y loguea.",
    ),
    ddl_only: Optional[bool] = typer.Option(
        False,
        "--ddl-only",
        help="Ejecuta reset + DDL y omite inserción (con --dry-run no hace nada).",
    ),
    debug_table_extract: Optional[str] = typer.Option(
        None,
        "--debug-table-extract",
        help="Si se provee, se muestra esta tabla después de la extracción.",
    ),
    debug_table_transform: Optional[str] = typer.Option(
        None,
        "--debug-table-transform",
        help="Si se provee, se muestra esta tabla después de la transformación.",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        help="Nivel de logging (INFO, DEBUG, ...).",
    ),
) -> None:
    """Ejecuta el pipeline ETL.

    Carga settings desde entorno/.env y permite sobrescribir valores desde flags.

    Args:
      excel_path: Ruta al archivo XLSX.
      pg_dsn: DSN SQLAlchemy para Postgres.
      schema: Nombre del schema destino.
      load_mode: Estrategia de reset del schema.
      include_sheets: String JSON con nombres de hojas a incluir.
      exclude_sheets: String JSON con nombres de hojas a excluir.
      dry_run: Si True, no escribe en Postgres.
      no_load: Si True, no carga (solo extract).

    Raises:
      ValueError: Si faltan settings requeridos (p.ej. pg_dsn cuando se carga).
      json.JSONDecodeError: Si include/exclude no son JSON válidos.
    """
    settings = ETLSettings()

    # Sobrescritura desde CLI cuando se provee.
    if excel_path is not None:
        settings.excel_path = str(excel_path)
    if pg_dsn is not None:
        settings.pg_dsn = pg_dsn
    if schema is not None:
        settings.pg_schema_core = schema
    if load_mode is not None:
        settings.load_mode = load_mode
    if exclude_sheets is not None:
        settings.exclude_sheets = json.loads(exclude_sheets)
    if dry_run is not None:
        settings.dry_run = dry_run
    if ddl_only is not None:
        settings.ddl_only = ddl_only
    if debug_table_extract is not None:
        settings.debug_table_extract = debug_table_extract
    if debug_table_transform is not None:
        settings.debug_table_transform = debug_table_transform
    if log_level is not None:
        settings.log_level = log_level
        setup_logging(settings.log_level)
        
    run_pipeline(settings=settings)


@app.command("config")
def config() -> None:
    """Imprime la configuración efectiva.

    Útil para verificar qué se tomó desde .env y qué quedó por defecto.
    """
    settings = ETLSettings()
    typer.echo(settings.model_dump_json(indent=2))


@app.command("list")
def list_sheets(
    excel_path: Optional[Path] = typer.Option(
        None,
        "--excel-path",
        dir_okay=False,
        help="Ruta al archivo .xlsx (si source=excel).",
    ),
) -> None:
    """Lista hojas disponibles en el origen configurado.

    Args:
      excel_path: Ruta al XLSX.

    Raises:
      ValueError: Si el origen está mal configurado.
    """
    settings = ETLSettings()
    if excel_path is not None:
        settings.excel_path = str(excel_path)

    from savh_etl.io.excel_client import ExcelClient

    for name in ExcelClient(settings.excel_path).list_sheets():
        typer.echo(name)


if __name__ == "__main__":
    app()
