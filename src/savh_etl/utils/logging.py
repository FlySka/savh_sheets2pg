"""Utilidades de logging.

Mantiene logging consistente en todos los módulos con un basicConfig simple.
"""

from __future__ import annotations

import logging
import os


def get_logger(name: str) -> logging.Logger:
    """Retorna un logger configurado.

    El nivel se toma desde:
      - SAVH_ETL_LOG_LEVEL (env var), por defecto "INFO"

    Args:
      name: Nombre del logger (normalmente __name__).

    Returns:
      Instancia de logging.Logger configurada.
    """
    level = os.getenv("SAVH_ETL_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger(name)
