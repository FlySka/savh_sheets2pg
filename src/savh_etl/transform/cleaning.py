"""Utilidades de limpieza y tipado para DataFrames.

Objetivo:
- Normalizar nombres de columnas (para SQL).
- Limpiar strings (trim, vacíos -> NA).
- Preparar columnas antes de casteos.

Nota:
Esta capa NO decide el esquema final (DDL). Solo prepara datos para:
- inspección en pandas
- carga rápida a Postgres
- futura carga robusta (DDL + COPY)
"""

from __future__ import annotations

import re
import pandas as pd
import unicodedata
from typing import Iterable, Optional

from savh_etl.utils.logging import get_logger
log = get_logger(__name__)


def empty2na(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte strings vacíos y placeholders comunes en NA.

    Args:
      df (pd.DataFrame): DataFrame.

    Returns:
      (pd.DataFrame): DataFrame con vacíos y placeholders convertidos a NA.
    """
    out = df.copy()
    placeholders = {"", "nan", "none", "null", "na", "n/a"}
    for c in out.columns:
        if out[c].dtype == "object" or str(out[c].dtype).startswith("string"):
            s = out[c].astype("string")
            out[c] = s.mask(s.str.lower().isin(placeholders), pd.NA)
    return out


_WHITESPACE_RE = re.compile(r"\s+")

def clean_string_series(s: pd.Series) -> pd.Series:
    """Limpia una serie de strings: trim, colapsa espacios y vacíos -> NA.

    Args:
      s (pd.Series): Serie pandas.

    Returns:
      (pd.Series): Serie limpia.
    """
    s = s.astype("string")
    s = s.str.strip()
    s = s.str.replace(_WHITESPACE_RE, " ", regex=True)
    s = s.mask(s == "", pd.NA)
    return s


def clean_strings(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    """Limpia columnas string (trim, colapsa espacios, vacíos -> NA).

    Args:
      df (pd.DataFrame): DataFrame.
      columns (list[str] | None): Columnas a limpiar. Si None, todas las string/objetos.

    Returns:
      (pd.DataFrame): DataFrame con conversiones aplicadas.
    """
    out = df.copy()
    cols = columns or [
        c for c in out.columns
        if out[c].dtype == "object" or str(out[c].dtype).startswith("string")
    ]
    for c in cols:
        out[c] = clean_string_series(out[c])
    return out

_NBSP = "\u00A0"
_UNNAMED_RE = re.compile(r"^unnamed(:\s*\d+)?$", flags=re.IGNORECASE)


def normalize_unicode_strings(
    df: pd.DataFrame,
    columns: Optional[Iterable[str]] = None,
    form: str = "NFKC",
    replace_nbsp: bool = True,
    replace_fancy_quotes: bool = True,
    replace_long_dashes: bool = True,
) -> pd.DataFrame:
    """Normaliza strings a nivel unicode para evitar caracteres invisibles/raros.

    Útil para datos que vienen desde Excel/Sheets con:
      - espacios no separables (NBSP)
      - comillas curvas (“ ” ‘ ’)
      - guiones largos (— –)
      - combinaciones unicode equivalentes (NFKC)

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        columns (Optional[Iterable[str]]): Columnas a procesar. Si None, procesa columnas object/string.
        form (str): Forma de normalización unicode (por defecto "NFKC").
        replace_nbsp (bool): Si True, reemplaza NBSP por espacio normal.
        replace_fancy_quotes (bool): Si True, reemplaza comillas curvas por comillas simples/dobles estándar.
        replace_long_dashes (bool): Si True, reemplaza guiones largos por "-".

    Returns:
        pd.DataFrame: Copia del DataFrame con normalización aplicada en las columnas objetivo.
    """
    out = df.copy()

    if columns is None:
        cols = [
            c
            for c in out.columns
            if out[c].dtype == "object" or str(out[c].dtype).startswith("string")
        ]
    else:
        cols = [c for c in columns if c in out.columns]

    if not cols:
        return out

    def _norm(x: object) -> object:
        if pd.isna(x):
            return pd.NA
        s = str(x)
        if replace_nbsp:
            s = s.replace(_NBSP, " ")
        s = unicodedata.normalize(form, s)
        if replace_fancy_quotes:
            s = (
                s.replace("“", '"')
                .replace("”", '"')
                .replace("‘", "'")
                .replace("’", "'")
            )
        if replace_long_dashes:
            s = s.replace("—", "-").replace("–", "-")
        return s

    for c in cols:
        out[c] = out[c].astype("string").map(_norm)

    return out


def normalize_placeholders(
    df: pd.DataFrame,
    columns: Optional[Iterable[str]] = None,
    placeholders: Optional[Iterable[str]] = None,
    casefold: bool = True,
    strip: bool = True,
) -> pd.DataFrame:
    """Convierte placeholders comunes (como '-', 's/i', 'no aplica') a NA.

    Esta función NO cambia el contenido "bueno"; solo mapea a NA si el valor coincide
    exactamente con alguno de los placeholders (tras strip y/o casefold).

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        columns (Optional[Iterable[str]]): Columnas a procesar. Si None, procesa columnas object/string.
        placeholders (Optional[Iterable[str]]): Lista custom de placeholders. Si None, usa un set por defecto.
        casefold (bool): Si True, compara en minúsculas robustas (casefold).
        strip (bool): Si True, hace trim antes de comparar.

    Returns:
        pd.DataFrame: Copia del DataFrame con placeholders convertidos a NA.
    """
    out = df.copy()

    default_placeholders = {
        "",
        "nan",
        "none",
        "null",
        "na",
        "n/a",
        "-",
        "--",
        ".",
        "..",
        "—",
        "s/i",
        "sin info",
        "sin información",
        "sin informacion",
        "sin dato",
        "no aplica",
        "no_aplica",
        "NO_APLICA",
        "n/a.",
    }
    ph = set(placeholders) if placeholders is not None else default_placeholders

    # Normalizamos placeholders para la comparación
    ph_norm = set()
    for p in ph:
        s = str(p)
        s = s.strip() if strip else s
        s = s.casefold() if casefold else s
        ph_norm.add(s)

    if columns is None:
        cols = [
            c
            for c in out.columns
            if out[c].dtype == "object" or str(out[c].dtype).startswith("string")
        ]
    else:
        cols = [c for c in columns if c in out.columns]

    for c in cols:
        s = out[c].astype("string")
        key = s
        if strip:
            key = key.str.strip()
        if casefold:
            key = key.str.casefold()
        out[c] = s.mask(key.isin(ph_norm), pd.NA)

    return out


def drop_empty_columns(
    df: pd.DataFrame,
    drop_unnamed: bool = True,
    drop_all_na: bool = True,
    drop_all_blank_strings: bool = True,
) -> pd.DataFrame:
    """Elimina columnas sin información (todo NA / todo vacío) y opcionalmente 'Unnamed:*'.

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        drop_unnamed (bool): Si True, elimina columnas cuyo nombre sea 'Unnamed' (típico de Excel).
        drop_all_na (bool): Si True, elimina columnas con todos los valores NA.
        drop_all_blank_strings (bool): Si True, considera strings vacíos/espacios como "vacío"
            y elimina columnas que sean solo vacíos (aunque no sean NA explícito).

    Returns:
        pd.DataFrame: Copia del DataFrame sin columnas vacías.
    """
    out = df.copy()
    to_drop: list[str] = []

    for c in out.columns:
        col = out[c]

        if drop_all_na and col.isna().all():
            to_drop.append(c)
            continue

        if drop_all_blank_strings and (
            col.dtype == "object" or str(col.dtype).startswith("string")
        ):
            s = col.astype("string")
            # Considera como vacío: NA o strings solo con espacios
            only_empty = s.isna() | (s.str.strip() == "")
            if bool(only_empty.all()):
                to_drop.append(c)
                continue

        if drop_unnamed:
            name = str(c).strip()
            if _UNNAMED_RE.match(name):
                # Se dropea siempre si es 'Unnamed', porque normalmente es basura de Excel.
                to_drop.append(c)

    if to_drop:
        out = out.drop(columns=to_drop)

    return out