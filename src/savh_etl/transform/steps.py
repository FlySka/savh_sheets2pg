"""
Define steps de transformación reutilizables. 
Cada step es una función que recibe un DataFrame y devuelve otro DataFrame transformado.
Estas funciones pueden ser combinadas en pipelines para aplicar múltiples transformaciones secuencialmente.
"""
from typing import List, Sequence, Union

import pandas as pd

from savh_etl.transform.pipeline import TransformFn
from savh_etl.transform.cleaning import (
    empty2na,
    clean_strings,
)
from savh_etl.transform.casting import (
    convert_booleans,
    clean_dates,
    clean_numbers,
    parser_ints,
)
from savh_etl.transform.mapping import (
    rename_values_to_ids,
    normalize_columns_names,
)
from savh_etl.transform.derive import (
    add_audit_columns,
    add_created_at,
    add_updated_at,
    add_deleted_at,
    add_deleted_by_user_id,
    add_ingest_event_id,
)

def step_normalize_columns_names() -> TransformFn:
    """Crea un step para normalizar nombres de columnas.

    Returns:
      (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return normalize_columns_names(df)

    return _fn

def step_empty2na() -> TransformFn:
    """Crea un step para convertir vacíos/placeholders a NA.

    Returns:
      (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return empty2na(df)

    return _fn

def step_clean_strings() -> TransformFn:
    """Crea un step para limpiar columnas de texto.

    Returns:
      (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return clean_strings(df)

    return _fn

def step_rename(mapping: dict[str, str]) -> TransformFn:
    """Crea un step para renombrar columnas.

    Args:
      mapping (dict[str, str]): Dict columna_origen -> columna_destino (ya normalizadas).
    Returns:
      (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=mapping)

    return _fn

def step_parse_dates(cols: list[str], *, dayfirst: bool = True) -> TransformFn:
    """Crea un step para parsear fechas.

    Args:
      cols (list[str]): Columnas a parsear.
      dayfirst (bool): Interpreta dd-mm-aaaa.

    Returns:
      (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return clean_dates(df, cols, dayfirst=dayfirst)

    return _fn

def step_parse_int(cols: list[str]) -> TransformFn:
    """Crea un step para parsear enteros nullable.

    Args:
      cols (list[str]): Columnas a convertir.

    Returns:
      (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return parser_ints(df, cols)

    return _fn

def step_parse_float(cols: list[str]) -> TransformFn:
    """Crea un step para parsear floats nullable.

    Args:
      cols (list[str]): Columnas a convertir.
    Returns:
      (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return clean_numbers(df, cols)

    return _fn

def step_parse_bool(cols: list[str]) -> TransformFn:
    """Crea un step para parsear booleanos nullable.

    Args:
      cols (list[str]): Columnas a convertir.

    Returns:
      (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return convert_booleans(df, cols)

    return _fn

def step_rename_values_to_ids(
    cols: List[str],
    *,
    table_name: Union[str, List[str]],
    column_lookup: Union[str, List[str]],
    tables: dict[str, pd.DataFrame],
    id_col: str = "id",
) -> TransformFn:
    """Crea un step que mapea múltiples columnas a *_id.

    Soporta:
      - table_name como string (se aplica a todas las cols)
      - table_name como lista (una por cada col)
    Igual para column_lookup.

    Ejemplos:
      1) Un mismo lookup para todas:
         step_rename_values_to_ids(
           cols=["tipo", "estado"],
           table_name="dim_estados",
           column_lookup="estado",
           tables=tables,
         )

      2) Un lookup distinto por columna:
         step_rename_values_to_ids(
           cols=["cliente", "vendedor"],
           table_name=["terceros", "vendedores"],
           column_lookup=["nombre", "nombre"],
           tables=tables,
         )

    Args:
      cols (List[str]): Columnas a transformar.
      table_name (Union[str, List[str]]): Nombre(s) de tabla(s) lookup
      column_lookup (Union[str, List[str]]): Nombre(s) de columna(s) lookup
      tables (dict[str, pd.DataFrame]): Tablas disponibles para lookups.
      id_col (str): Nombre de la columna ID en la tabla lookup.

    Returns:
      (TransformFn): Función TransformFn.
    """
    def _as_list(x: Union[str, Sequence[str]], n: int, label: str) -> list[str]:
        if isinstance(x, str):
            return [x] * n
        x_list = list(x)
        if len(x_list) == 1 and n > 1:
            return x_list * n
        if len(x_list) != n:
            raise ValueError(f"{label} debe tener largo 1 o {n}. Recibido: {len(x_list)}")
        return x_list

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        out = df
        tnames = _as_list(table_name, len(cols), "table_name")
        lcols  = _as_list(column_lookup, len(cols), "column_lookup")

        for c, tn, lk in zip(cols, tnames, lcols):
            out = rename_values_to_ids(
                out,
                column=c,
                table_name=tn,
                column_lookup=lk,
                tables=tables,
                id_col=id_col,
            ).df
        return out

    return _fn

def step_add_created_at(
    *,
    value: pd.Timestamp | None = None,
    overwrite: bool = False,
    col: str = "created_at",
    tz: str = "UTC",
) -> TransformFn:
    """Crea un step para agregar created_at.
    
    Args:
        value (pd.Timestamp | None): Valor a asignar. Si None, usa CURRENT_TIMESTAMP
        overwrite (bool): Si True, sobrescribe si la columna ya existe.
        col (str): Nombre de la columna a agregar.
        tz (str): Zona horaria para CURRENT_TIMESTAMP.
        
    Returns:
        (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return add_created_at(df, value=value, overwrite=overwrite, col=col, tz=tz)

    return _fn

def step_add_updated_at(
    *,
    value: pd.Timestamp | None = None,
    overwrite: bool = False,
    col: str = "updated_at",
) -> TransformFn:
    """Crea un step para agregar updated_at.
    
    Args:
        value (pd.Timestamp | None): Valor a asignar. Si None, usa NULL.
        overwrite (bool): Si True, sobrescribe si la columna ya existe.
        col (str): Nombre de la columna a agregar.
        
    Returns:
        (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return add_updated_at(df, value=value, overwrite=overwrite, col=col)

    return _fn

def step_add_deleted_at(
    *,
    value: pd.Timestamp | None = None,
    overwrite: bool = False,
    col: str = "deleted_at",
) -> TransformFn:
    """Crea un step para agregar deleted_at.
    
    Args:
        value (pd.Timestamp | None): Valor a asignar. Si None, usa NULL.
        overwrite (bool): Si True, sobrescribe si la columna ya existe.
        col (str): Nombre de la columna a agregar.
        
    Returns:
        (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return add_deleted_at(df, value=value, overwrite=overwrite, col=col)

    return _fn

def step_add_deleted_by_user_id(
    *,
    value: int | pd.Series | None = None,
    overwrite: bool = False,
    col: str = "deleted_by_user_id",
) -> TransformFn:
    """Crea un step para agregar deleted_by_user_id.
    
    Args:
        """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return add_deleted_by_user_id(df, value=value, overwrite=overwrite, col=col)

    return _fn

def step_add_ingest_event_id(
    *,
    value: int | pd.Series | None = None,
    overwrite: bool = False,
    col: str = "ingest_event_id",
) -> TransformFn:
    """Crea un step para agregar ingest_event_id.
    
    Args:
        value (int | pd.Series | None): Valor a asignar. Si None, usa
        overwrite (bool): Si True, sobrescribe si la columna ya existe.
        col (str): Nombre de la columna a agregar.
        
    Returns:
        (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return add_ingest_event_id(df, value=value, overwrite=overwrite, col=col)

    return _fn

def step_add_audit_columns(
    *,
    created_at: pd.Timestamp | None = None,
    updated_at: pd.Timestamp | None = None,
    deleted_at: pd.Timestamp | None = None,
    deleted_by_user_id: int | pd.Series | None = None,
    ingest_event_id: int | pd.Series | None = None,
    overwrite: bool = False,
    tz: str = "UTC",
) -> TransformFn:
    """Crea un step para agregar columnas de auditoría estándar.
    
    Args:
        created_at (pd.Timestamp | None): Valor para created_at. Si None, usa CURRENT_TIMESTAMP.
        updated_at (pd.Timestamp | None): Valor para updated_at. Si None, usa NULL.
        deleted_at (pd.Timestamp | None): Valor para deleted_at. Si None, usa NULL.
        deleted_by_user_id (int | pd.Series | None): Valor para deleted_by_user_id. Si None, usa NULL.
        ingest_event_id (int | pd.Series | None): Valor para ingest_event_id. Si None, usa NULL.
        overwrite (bool): Si True, sobrescribe columnas si ya existen.
        tz (str): Zona horaria para CURRENT_TIMESTAMP.
        
    Returns:
        (TransformFn): Función TransformFn.
    """

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return add_audit_columns(
            df,
            created_at=created_at,
            updated_at=updated_at,
            deleted_at=deleted_at,
            deleted_by_user_id=deleted_by_user_id,
            ingest_event_id=ingest_event_id,
            overwrite=overwrite,
            tz=tz,
        )

    return _fn
