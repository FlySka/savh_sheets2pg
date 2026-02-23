"""Capa de extracción.

Convierte el origen (Excel/Sheets) en una estructura en memoria:
  Dict[str, DataFrame], donde las llaves son nombres normalizados de tablas.

Responsabilidades:
  - listar hojas del origen
  - filtrar por include/exclude
  - normalizar nombres (identificadores SQL)
  - aplicar reglas de naming para tablas SQL:
      - "detalle_*" -> "*_items"
  - leer cada hoja como DataFrame (dtype=str para estabilidad)
"""

from __future__ import annotations

import re
from typing import Dict, List

import pandas as pd

from savh_etl.io.excel_client import ExcelClient
from savh_etl.settings import ETLSettings
from savh_etl.utils.logging import get_logger
log = get_logger(__name__)


def _filter_sheet_names(all_names: List[str], settings: ETLSettings) -> List[str]:
    """Aplica filtros include/exclude y reglas comunes de ruido.

    Args:
      all_names: Lista completa de hojas del origen.
      settings: Settings del ETL.

    Returns:
      Lista filtrada de hojas.
    """
    names = list(all_names)

    if settings.exclude_sheets:
        exclude_set = set(settings.exclude_sheets)
        names = [n for n in names if n not in exclude_set]

    # Convención: ignorar hojas utilitarias que empiezan con "_".
    names = [n for n in names if not n.strip().startswith("_")]

    return names


def extract(settings: ETLSettings) -> Dict[str, pd.DataFrame]:
    """Extrae todas las hojas seleccionadas como DataFrames.

    Args:
      settings: Settings del ETL.

    Returns:
      Diccionario: nombre_tabla_sql -> DataFrame.

    Raises:
      ValueError: Si el origen no es soportado o está mal configurado.
    """
    all_names = ExcelClient(settings.excel_path).list_sheets()
    names = _filter_sheet_names(all_names, settings)

    log.info("Extrayendo hojas (%d): %s", len(names), ", ".join(names))

    tables: Dict[str, pd.DataFrame] = {}

    client = ExcelClient(settings.excel_path)
    for sheet_name in names:
        df = client.read_sheet(sheet_name)
        tables[sheet_name] = df
    return tables
