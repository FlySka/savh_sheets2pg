"""Utilidades para derivar columnas en dataframes.
Aquí se definen funciones que permiten crear nuevas columnas
basadas en transformaciones o combinaciones de columnas existentes.
Estas funciones son útiles para enriquecer los datos
durante el proceso ETL.
"""
from __future__ import annotations

import pandas as pd

from savh_etl.utils.logging import get_logger
log = get_logger(__name__)


def add_created_at(
    df: pd.DataFrame,
    *,
    value: pd.Timestamp | None = None,
    overwrite: bool = False,
    col: str = "created_at",
    tz: str = "UTC",
) -> pd.DataFrame:
    """Agrega columna created_at con default CURRENT_TIMESTAMP.

    Args:
        df (pd.DataFrame): DataFrame original.
        value (pd.Timestamp | None): Timestamp fijo. Si es None usa now().
        overwrite (bool): Si sobrescribir columna si ya existe.
        col (str): Nombre de la columna destino.
        tz (str): Zona horaria para now() cuando value es None.

    Returns:
        pd.DataFrame: DataFrame con created_at agregado.
    """
    out = df.copy()
    if col in out.columns and not overwrite:
        return out
    ts = value if value is not None else pd.Timestamp.now(tz=tz)
    out[col] = pd.to_datetime(ts, utc=True)
    return out

def add_updated_at(
    df: pd.DataFrame,
    *,
    value: pd.Timestamp | None = None,
    overwrite: bool = False,
    col: str = "updated_at",
) -> pd.DataFrame:
    """Agrega columna updated_at nullable.

    Args:
        df (pd.DataFrame): DataFrame original.
        value (pd.Timestamp | None): Timestamp fijo o None (NULL).
        overwrite (bool): Si sobrescribir columna si ya existe.
        col (str): Nombre de la columna destino.

    Returns:
        pd.DataFrame: DataFrame con updated_at agregado.
    """
    out = df.copy()
    if col in out.columns and not overwrite:
        return out
    ts = pd.NaT if value is None else value
    out[col] = pd.to_datetime(ts, utc=True)
    return out

def add_deleted_at(
    df: pd.DataFrame,
    *,
    value: pd.Timestamp | None = None,
    overwrite: bool = False,
    col: str = "deleted_at",
) -> pd.DataFrame:
    """Agrega columna deleted_at nullable.

    Args:
        df (pd.DataFrame): DataFrame original.
        value (pd.Timestamp | None): Timestamp fijo o None (NULL).
        overwrite (bool): Si sobrescribir columna si ya existe.
        col (str): Nombre de la columna destino.

    Returns:
        pd.DataFrame: DataFrame con deleted_at agregado.
    """
    out = df.copy()
    if col in out.columns and not overwrite:
        return out
    ts = pd.NaT if value is None else value
    out[col] = pd.to_datetime(ts, utc=True)
    return out

def add_deleted_by_user_id(
    df: pd.DataFrame,
    *,
    value: int | pd.Series | None = None,
    overwrite: bool = False,
    col: str = "deleted_by_user_id",
) -> pd.DataFrame:
    """Agrega columna deleted_by_user_id nullable.

    Args:
        df (pd.DataFrame): DataFrame original.
        value (int | pd.Series | None): Valor fijo o serie con IDs.
        overwrite (bool): Si sobrescribir columna si ya existe.
        col (str): Nombre de la columna destino.

    Returns:
        pd.DataFrame: DataFrame con deleted_by_user_id agregado.
    """
    out = df.copy()
    if col in out.columns and not overwrite:
        return out
    out[col] = pd.NA if value is None else value
    out[col] = out[col].astype("Int64")
    return out

def add_ingest_event_id(
    df: pd.DataFrame,
    *,
    value: int | pd.Series | None = None,
    overwrite: bool = False,
    col: str = "ingest_event_id",
) -> pd.DataFrame:
    """Agrega columna ingest_event_id nullable.

    Args:
        df (pd.DataFrame): DataFrame original.
        value (int | pd.Series | None): Valor fijo o serie con IDs.
        overwrite (bool): Si sobrescribir columna si ya existe.
        col (str): Nombre de la columna destino.

    Returns:
        pd.DataFrame: DataFrame con ingest_event_id agregado.
    """
    out = df.copy()
    if col in out.columns and not overwrite:
        return out
    out[col] = pd.NA if value is None else value
    out[col] = out[col].astype("Int64")
    return out

def add_audit_columns(
    df: pd.DataFrame,
    *,
    created_at: pd.Timestamp | None = None,
    updated_at: pd.Timestamp | None = None,
    deleted_at: pd.Timestamp | None = None,
    deleted_by_user_id: int | pd.Series | None = None,
    ingest_event_id: int | pd.Series | None = None,
    overwrite: bool = False,
    tz: str = "UTC",
) -> pd.DataFrame:
    """Agrega columnas de auditoría estándar.

    Reglas:
        - created_at: default CURRENT_TIMESTAMP (tz-aware)
        - updated_at/deleted_at: NULL
        - deleted_by_user_id/ingest_event_id: NULL

    Args:
        df (pd.DataFrame): DataFrame original.
        created_at (pd.Timestamp | None): Valor fijo o None para now().
        updated_at (pd.Timestamp | None): Valor fijo o None.
        deleted_at (pd.Timestamp | None): Valor fijo o None.
        deleted_by_user_id (int | pd.Series | None): Valor fijo o serie.
        ingest_event_id (int | pd.Series | None): Valor fijo o serie.
        overwrite (bool): Si sobrescribir columnas si ya existen.
        tz (str): Zona horaria para now() cuando created_at es None.

    Returns:
        pd.DataFrame: DataFrame con columnas de auditoría agregadas.
    """
    out = df
    out = add_created_at(out, value=created_at, overwrite=overwrite, tz=tz)
    out = add_updated_at(out, value=updated_at, overwrite=overwrite)
    out = add_deleted_at(out, value=deleted_at, overwrite=overwrite)
    out = add_deleted_by_user_id(out, value=deleted_by_user_id, overwrite=overwrite)
    out = add_ingest_event_id(out, value=ingest_event_id, overwrite=overwrite)
    return out
