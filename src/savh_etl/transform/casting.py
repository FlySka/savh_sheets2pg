"""Utilidades para casteo de tipos en DataFrames pandas.

Objetivo:
- Proveer funciones para castear columnas típicas (booleanos, fechas, números, IDs).
- Estas funciones pueden ser usadas en steps y pipelines de transformación.
"""

from __future__ import annotations

import re
import pandas as pd
from typing import Iterable

from savh_etl.utils.logging import get_logger
log = get_logger(__name__)



def _parse_bool_serie(s: pd.Series) -> pd.Series:
    """Parsea una serie a booleano nullable (boolean) si aplica.

    Args:
      s (pd.Series): Serie pandas.

    Returns:
      (pd.Series): Serie casteada a booleano nullable.
    """
    mapa = {
        "true": True,
        "false": False,
        "1": True,
        "0": False,
        "si": True,
        "sí": True,
        "no": False,
        "y": True,
        "n": False,
        "t": True,
        "f": False,
    }
    ss = s.astype("string").str.strip().str.lower()
    out = ss.map(mapa)
    # Mantiene NA donde no matchea.
    return out.astype("boolean")


def convert_booleans(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    """Convierte columnas específicas a booleano nullable.

    Args:
      df (pd.DataFrame): DataFrame.
      cols (Iterable[str]): Columnas a convertir si existen.

    Returns:
      (pd.DataFrame): DataFrame con conversiones aplicadas.
    """
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = _parse_bool_serie(out[c])
    return out


def _clean_number_str(x: str) -> str:
    """Limpia un número representado como string para parseo robusto.

    Reglas:
      - elimina símbolos/currency/letras
      - maneja separadores miles/decimales típicos (., ,)

    Args:
      x (str): String crudo.

    Returns:
      (str): String limpio listo para parseo numérico.
    """
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return ""

    # Deja solo dígitos, coma, punto y signo.
    s = re.sub(r"[^0-9,\.\-]", "", s)

    # Si trae "." y "," a la vez: asumimos "." miles y "," decimal -> quitamos "." y cambiamos "," por "."
    if "." in s and "," in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
        return s

    # Si solo trae "," -> decimal
    if "," in s and "." not in s:
        s = s.replace(",", ".")
        return s

    # Si solo trae "." -> ya está OK (o miles sin decimales; igual to_numeric decide).
    return s


def clean_number_series(
    s: pd.Series,
    *,
    decimal: str = ",",
    thousands: str | None = ".",
) -> pd.Series:
    """Limpia una serie numérica representada como string y la castea a número.

    Args:
      s (pd.Series): Serie pandas.
      decimal (str): Separador decimal en los strings.
      thousands (str | None): Separador de miles en los strings.

    Returns:
      (pd.Series): Serie numérica limpia y casteada.
    """
    s = s.astype("string")
    s = s.str.strip()
    # Eliminar espacios y separadores de miles
    s = s.str.replace(r"[\s\u00A0]", "", regex=True)
    percent_mask = s.str.contains("%", na=False)
    s = s.str.replace("%", "", regex=False)

    if thousands:
        # Si decimal es "," (latam) pero vienen números con "." decimal (típico al leer floats desde Excel),
        # NO debemos eliminar el "." salvo que exista también "," (caso 1.234,56).
        if thousands == "." and decimal == ",":
            has_comma = s.str.contains(",", na=False)
            # Si hay más de un "." sin coma, es casi seguro separador de miles (ej: 12.345.678).
            multi_dot = s.str.count(r"\.").fillna(0) > 1
            strip_thousands = has_comma | multi_dot
            s = s.where(~strip_thousands, s.str.replace(".", "", regex=False))
        else:
            s = s.str.replace(re.escape(thousands), "", regex=True)
    if decimal != ".":
        s = s.str.replace(decimal, ".", regex=False)
    s = s.mask(s == "", pd.NA)
    out = pd.to_numeric(s, errors="coerce")
    if bool(percent_mask.any()):
        out = out.where(~percent_mask, out / 100)
    return out


def clean_numbers(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    *,
    type_name: str = "float",
    decimal: str = ",",
    thousands: str | None = ".",
) -> pd.DataFrame:
    """Limpia columnas numéricas en formato string y las castea a número.

    Args:
      df (pd.DataFrame): DataFrame.
      columns (list[str] | None): Columnas a convertir. Si None, todas las string/objetos.
      type_name (str): Tipo destino (float, int, etc.).
      decimal (str): Separador decimal en los strings.
      thousands (str | None): Separador de miles en los strings.

    Returns:
      (pd.DataFrame): DataFrame con conversiones aplicadas.
    """
    out = df.copy()
    cols = columns or [
        c for c in out.columns
        if out[c].dtype == "object" or str(out[c].dtype).startswith("string")
    ]
    for c in cols:
        out[c] = clean_number_series(out[c], decimal=decimal, thousands=thousands).astype(type_name)
    return out


def clean_date_series(
    s: pd.Series,
    *,
    dayfirst: bool = True,
) -> pd.Series:
    """Limpia una serie de fechas en string y la convierte a datetime.

    Args:
      s (pd.Series): Serie pandas.
      dayfirst (bool): Interpreta el día antes que el mes.

    Returns:
      (pd.Series): Serie con dtype datetime (coerción en errores).
    """
    s = s.astype("string")
    s = s.str.strip()
    s = s.mask(s == "", pd.NA)
    if dayfirst:
        iso_like = s.str.match(
            r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?$",
            na=False,
        )
        if bool(iso_like.any()):
            iso_parsed = pd.to_datetime(s.where(iso_like), errors="coerce", dayfirst=False)
            other_parsed = pd.to_datetime(s.where(~iso_like), errors="coerce", dayfirst=True)
            return iso_parsed.fillna(other_parsed)

    return pd.to_datetime(s, errors="coerce", dayfirst=dayfirst)


def clean_dates(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    *,
    dayfirst: bool = True,
) -> pd.DataFrame:
    """Limpia columnas fecha en formato string y las castea a datetime.

    Args:
      df (pd.DataFrame): DataFrame.
      columns (list[str] | None): Columnas a convertir. Si None, todas las string/objetos.
      dayfirst (bool): Interpreta el día antes que el mes.

    Returns:
      (pd.DataFrame): DataFrame con conversiones aplicadas.
    """
    out = df.copy()
    cols = columns or [
        c for c in out.columns
        if out[c].dtype == "object" or str(out[c].dtype).startswith("string")
    ]
    for c in cols:
        out[c] = clean_date_series(out[c], dayfirst=dayfirst)
    return out


def parser_ints(
    df: pd.DataFrame,
    cols: Iterable[str],
    dtype: str = "Int64",
) -> pd.DataFrame:
    """Convierte columnas a entero nullable.

    Útil para ids y *_id.

    Args:
      df (pd.DataFrame): DataFrame.
      cols (Iterable[str]): Columnas a convertir si existen.
      dtype (str): Dtype pandas nullable ("Int64" recomendado).

    Returns:
      (pd.DataFrame): DataFrame con conversiones aplicadas.
    """
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            continue
        s = out[c].astype("string").map(_clean_number_str)
        out[c] = pd.to_numeric(s, errors="coerce").astype(dtype)
    return out

def parser_floats(
    df: pd.DataFrame,
    cols: Iterable[str],
    dtype: str = "Float64",
    decimal: str = ",",
    thousands: str | None = ".",
) -> pd.DataFrame:
    """Convierte columnas a float nullable.

    Args:
      df (pd.DataFrame): DataFrame.
      cols (Iterable[str]): Columnas a convertir si existen.
      dtype (str): Dtype pandas nullable ("Float64" recomendado).
      decimal (str): Separador decimal en los strings.
      thousands (str | None): Separador de miles en los strings.

    Returns:
      (pd.DataFrame): DataFrame con conversiones aplicadas.
    """
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = clean_number_series(
                out[c],
                decimal=decimal,
                thousands=thousands,
            ).astype(dtype)
    return out
