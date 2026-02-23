from __future__ import annotations

import logging
from rich.logging import RichHandler


def setup_logging(log_level: str = "INFO") -> None:
    """Configura logging con Rich para consola.

    Args:
        log_level (str): Nivel de logging (INFO, DEBUG, WARNING, ERROR).
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[
            RichHandler(
                rich_tracebacks=True,
                markup=True,
                show_time=True,
                show_level=True,
                show_path=True,
            )
        ],
        force=True,
    )
