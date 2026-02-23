"""
Funciones para mapeo de valores en DataFrames.
Aquí se definen funciones que permiten mapear valores de columnas
a sus correspondientes IDs usando tablas de referencia (lookup tables).
"""

from __future__ import annotations

import re
import pandas as pd
from dataclasses import dataclass

from savh_etl.utils.logging import get_logger
log = get_logger(__name__)

_COL_RE = re.compile(r"[^a-z0-9_]+")

def normalize_table_name(name: str) -> str:
    """Normaliza un string para que sea un nombre de tabla SQL seguro.

    Reglas:
      - Minúsculas
      - Espacios/puntuación -> "_"
      - Colapsa "_" repetidos
      - Regla de negocio: si el nombre queda como "detalle_*",
        entonces se transforma a "*_items".
      - Regla de negocio: si el nombre empieza con "cat_", se reemplaza por "dim_".
    Ejemplos:
      - "DETALLE COMPRAS PRODUCTO" -> "compras_producto_items"
      - "CATÁLOGO_VENTAS" -> "dim_ventas"
      - "COMPRAS_PRODUCTO" -> "compras_producto"

    Args:
      name (str): Nombre crudo de hoja.

    Returns:
      (str): Identificador normalizado (minúsculas y guiones bajos), con reglas aplicadas.
    """
    n = name.strip().lower().replace(" ", "_")
    n = _COL_RE.sub("_", n)
    n = re.sub(r"_+", "_", n).strip("_")

    # Regla: "detalle_*" -> "*_items"
    # Nota: aplica solo si "detalle_" está al inicio del nombre final.
    if n.startswith("detalle_"):
        n = n.removeprefix("detalle_")
        n = f"{n}_items" if n else "items"

    # Regla: cat_vehiculos -> vehiculos
    if n == "cat_vehiculos":
        n = "vehiculos"

    # Regla: cat_* -> dim_*
    if n.startswith("cat_"):
        n = n.replace("cat_", "dim_", 1)

    return n

def normalize_tables_names(dict_df: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Normaliza nombres de tablas.

    Args:
        dict_df (dict[str, pd.DataFrame]): Diccionario tabla -> DataFrame.

    Returns:
        dict[str, pd.DataFrame]: Diccionario con nombres de tabla normalizados.
    """
    out = {}
    for table_name, table_df in dict_df.items():
        norm_name = normalize_table_name(table_name)
        if norm_name in out:
            raise ValueError(f"Nombre de tabla normalizado duplicado: {norm_name}")
        out[norm_name] = table_df
        out[norm_name].name = norm_name
    return out

def normalize_column_name(name: str) -> str:
    """Normaliza un string para que sea un identificador SQL seguro.

    Reglas:
      - Minúsculas
      - Espacios/puntuación -> "_"
      - Colapsa "_" repetidos
      - Regla de negocio: si el nombre queda como "detalle_*",
        entonces se transforma a "*_items".

    Ejemplos:
      - "DETALLE COMPRAS PRODUCTO" -> "compras_producto_items"
      - "DETALLE_VENTAS" -> "ventas_items"
      - "COMPRAS_PRODUCTO" -> "compras_producto"

    Args:
      name (str): Nombre crudo de hoja.

    Returns:
      (str): Identificador normalizado (minúsculas y guiones bajos), con reglas aplicadas.
    """
    n = name.strip().lower().replace(" ", "_")
    n = _COL_RE.sub("_", n)
    n = re.sub(r"_+", "_", n).strip("_")

    # regla: activo" -> "is_active"
    if n == "activo":
        n = "is_active"

    return n


def normalize_columns_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres de columna y verifica que no haya nombres duplicados.

    Args:
      df (pd.DataFrame): DataFrame original.

    Returns:
      (pd.DataFrame): Copia del DataFrame con columnas normalizadas.

    Raises:
      ValueError: Si hay columnas que normalizan al mismo nombre.
    """
    out = df.copy()

    cols = [normalize_column_name(c) for c in out.columns]
    seen: dict[str, int] = {}

    for c in cols:
        if c in seen:
            raise ValueError(f"Columna normalizada duplicada: {c}")
        seen[c] = 1

    out.columns = cols
    return out

@dataclass(frozen=True)
class RenameToIdResult:
    df: pd.DataFrame
    matched: int
    unmatched: int
    unmatched_examples: list[str]


def rename_values_to_ids(
    df: pd.DataFrame,
    *,
    column: str,
    table_name: str,
    column_lookup: str,
    tables: dict[str, pd.DataFrame],
    id_col: str = "id",
) -> RenameToIdResult:
    """Crea una NUEVA columna '{column}_id' mapeando df[column] -> lookup[id_col].

    - Elimina df[column].
    - Siempre escribe/reescribe df[f"{column}_id"].
    - Falla si hay duplicados en lookup tras normalización o si hay valores sin mapear.

    Args:
        df (pd.DataFrame): DataFrame.
        column (str): Columna en df con valores a mapear.
        table_name (str): Nombre de la tabla de lookup.
        column_lookup (str): Columna en la tabla de lookup para buscar valores.
        tables (dict[str, pd.DataFrame]): Diccionario con tablas disponibles.
        id_col (str): Columna en la tabla de lookup con los ids.

    Returns:
        (RenameToIdResult): Resultado con DataFrame modificado y estadísticas.

    Examples:
    >>> result = rename_values_to_ids(
    >>>    df=my_df,
    >>>    column="vendedor",
    >>>    table_name="vendedores",
    >>>    column_lookup="nombre",
    >>>    tables=lookup_tables,
    >>> )
    >>> df_transformed = result.df 
    """
    out = df.copy()

    if column not in out.columns:
        return RenameToIdResult(out, matched=0, unmatched=0, unmatched_examples=[])

    tgt = f"{column}_id"

    if table_name not in tables or not isinstance(tables[table_name], pd.DataFrame):
        raise ValueError(f"Tabla lookup no disponible o inválida: '{table_name}'")

    lookup = tables[table_name]

    if column_lookup not in lookup.columns:
        raise ValueError(f"Lookup '{table_name}' no tiene columna '{column_lookup}'")
    if id_col not in lookup.columns:
        raise ValueError(f"Lookup '{table_name}' no tiene columna '{id_col}'")

    def _norm(s: pd.Series) -> pd.Series:
        # strip + casefold + vacíos -> NA
        ss = s.astype("string").str.strip().str.casefold()
        return ss.mask(ss == "", pd.NA)

    left = _norm(out[column])

    lu = lookup[[column_lookup, id_col]].copy()
    lu["_key"] = _norm(lu[column_lookup])
    lu = lu.dropna(subset=["_key", id_col])

    # Evitar mapeo ambiguo
    if lu["_key"].duplicated().any():
        dup = lu.loc[lu["_key"].duplicated(), "_key"].unique().tolist()
        raise ValueError(
            f"Lookup '{table_name}' tiene duplicados en '{column_lookup}' tras normalización. Ej: {dup[:10]}"
        )

    mapping = pd.Series(lu[id_col].values, index=lu["_key"].values)
    mapped = left.map(mapping)

    out[tgt] = mapped
    out = out.drop(columns=[column])

    matched = int(mapped.notna().sum())
    unmatched_mask = left.notna() & mapped.isna()
    unmatched = int(unmatched_mask.sum())
    unmatched_examples = [str(x) for x in left[unmatched_mask].dropna().unique().tolist()[:10]]

    if unmatched > 0:
        raise ValueError(
            f"No se pudieron mapear {unmatched} valores en '{column}' usando "
            f"lookup '{table_name}.{column_lookup}' -> '{id_col}'. Ej: {unmatched_examples}"
        )

    return RenameToIdResult(out, matched=matched, unmatched=unmatched, unmatched_examples=unmatched_examples)

def mapping_terceros_ids(
    all_tables: dict[str, pd.DataFrame],
    tercero_tables: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Reemplaza cliente_id / proveedor_id / vendedor_id en TODAS las tablas,
    mapeando ids antiguos -> terceros.id según terceros.id_old (ej: 'clientes:39').

    - Soporta múltiples tokens en id_old: 'clientes:39|proveedores:3'
    - Falla si hay valores no mapeables (no-null) o keys duplicadas ambiguas.
    """
    if "terceros" not in tercero_tables:
        raise KeyError("tercero_tables debe incluir la tabla 'terceros'")

    terceros = tercero_tables["terceros"].copy()
    if "id" not in terceros.columns or "id_old" not in terceros.columns:
        raise KeyError("terceros debe tener columnas 'id' y 'id_old'")

    # --- 1) Explode de terceros.id_old -> (source, old_id) -> tercero_id ---
    t = terceros[["id", "id_old"]].dropna(subset=["id_old"]).copy()
    t["id_old"] = t["id_old"].astype("string").str.split("|")
    t = t.explode("id_old").dropna(subset=["id_old"])

    parts = t["id_old"].astype("string").str.split(":", n=1, expand=True)
    if parts.shape[1] != 2:
        raise ValueError("Formato inválido en terceros.id_old (se espera 'source:id')")

    t["source"] = parts[0].astype("string").str.strip()
    t["old_id"] = pd.to_numeric(parts[1], errors="coerce").astype("Int64")
    t = t.dropna(subset=["source", "old_id"])

    # --- 2) Construir mapping por source (clientes/proveedores/vendedores) ---
    mappings: dict[str, pd.Series] = {}
    for src, g in t.groupby("source"):
        # detecta duplicados ambiguos: mismo old_id -> distinto tercero_id
        amb = g.groupby("old_id")["id"].nunique()
        amb = amb[amb > 1]
        if len(amb) > 0:
            ex = amb.index.astype(int).tolist()[:10]
            raise ValueError(f"Mapping ambiguo en terceros.id_old para source='{src}'. old_id duplicados: {ex}")

        mappings[src] = g.drop_duplicates(subset=["old_id"]).set_index("old_id")["id"]

    # qué columna -> qué source
    col_to_source = {
        "cliente_id": "clientes",
        "proveedor_id": "proveedores",
        "vendedor_id": "vendedores",
    }

    # --- 3) Aplicar mapping en todas las tablas ---
    out: dict[str, pd.DataFrame] = {}
    for table_name, df in all_tables.items():
        dfo = df.copy()

        for col, src in col_to_source.items():
            if col not in dfo.columns:
                continue

            if src not in mappings:
                raise ValueError(f"No existe mapping para source='{src}' (no apareció en terceros.id_old)")

            s = pd.to_numeric(dfo[col], errors="coerce").astype("Int64")
            mapped = s.map(mappings[src]).astype("Int64")

            # unmatched = valores que venían (no-null) pero no mapearon
            unmatched_mask = s.notna() & mapped.isna()
            if unmatched_mask.any():
                examples = s[unmatched_mask].dropna().unique().tolist()[:10]
                raise ValueError(
                    f"[{table_name}.{col}] No se pudieron mapear {int(unmatched_mask.sum())} valores "
                    f"({src} -> terceros). Ejemplos: {examples}"
                )

            dfo[col] = mapped

        out[table_name] = dfo

    log.info("Mapping terceros OK: cliente_id/proveedor_id/vendedor_id -> terceros.id")
    return out