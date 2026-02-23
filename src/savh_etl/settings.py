"""Configuración del proyecto ETL.

Usa pydantic-settings para cargar configuración desde:
  - variables de entorno
  - archivo .env (opcional)

Prefijo de entorno:
  SAVH_ETL_

Ejemplo:
  SAVH_ETL_SOURCE=excel
  SAVH_ETL_EXCEL_PATH=/mnt/data/BD_SAVH.xlsx
  SAVH_ETL_PG_DSN=postgresql+psycopg://user:pass@localhost:5432/savh
"""

from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict



class LoadMode(str, Enum):
    """Estrategias soportadas para reset del schema."""

    drop_create = "drop_create"
    truncate = "truncate"


class ETLSettings(BaseSettings):
    """Configuración de ejecución del ETL.

    Attributes:
      source: Tipo de origen para extracción.
      excel_path: Ruta al XLSX cuando source=excel.
      include_sheets: Lista opcional de hojas a incluir.
      exclude_sheets: Lista de hojas a excluir.
      pg_dsn: DSN SQLAlchemy de Postgres.
      pg_schema_core: Schema core destino en Postgres.
      pg_schema_ingest: Schema ingest (técnico) en Postgres.
      pg_schema_audit: Schema audit (técnico) en Postgres.
      load_mode: Estrategia de reset del schema.
      pg_chunksize: Tamaño de chunk para inserciones con to_sql.
      ddl_only: Si True, ejecuta solo DDL (sin insertar data).
      dry_run: Si True, no escribe en Postgres.
      log_level: Nivel de logging (INFO, DEBUG, ...).
    """

    model_config = SettingsConfigDict(
        env_prefix="SAVH_ETL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Excel: por defecto, el archivo que subiste en este entorno.
    excel_path: str = Field(
        default="/mnt/c/Users/joaqu/Desktop/SAVH/AUTOMATIZACIONES/savh_sheets2pg/BD_SAVH.xlsx",
        description="Ruta al archivo .xlsx",
    )

    # Selección de hojas
    exclude_sheets: List[str] = Field(default_factory=list)

    # Postgres
    pg_dsn: Optional[str] = None
    pg_schema_core: str = "core"
    pg_schema_ingest: str = "ingest"
    pg_schema_audit: str = "audit"
    load_mode: LoadMode = LoadMode.drop_create
    pg_chunksize: int = 5000
    # Compat env vars:
    # - SAVH_ETL_ONLY_DDL (preferido)
    # - SAVH_ETL_DDL_ONLY (legacy)
    ddl_only: bool = Field(
        default=False,
        validation_alias=AliasChoices("ONLY_DDL", "DDL_ONLY"),
    )

    # General
    dry_run: bool = True
    debug_table_extract: Optional[str] = None
    debug_table_transform: Optional[str] = None
    log_level: str = "INFO"
