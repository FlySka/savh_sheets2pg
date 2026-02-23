"""Utilidades para contruir tablas nuevas con logica de negocio.
Aquí se definen funciones que combinan y transforman dataframes
existentes para crear nuevas tablas derivadas durante el proceso ETL.
Estas funciones encapsulan la lógica de negocio específica
necesaria para construir estas tablas.
"""

from __future__ import annotations

import pandas as pd
from savh_etl.utils.logging import get_logger
log = get_logger(__name__)



def build_terceros_tables(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Construye:
      - terceros (con id_old temporal)
      - terceros_cliente
      - terceros_proveedor
      - terceros_vendedor

    Reglas clave:
      - Genera PK incremental `terceros.id`.
      - Mantiene trazabilidad con `id_old` (ej: "clientes:12|proveedores:3").
      - No elimina tablas raw

    Args:   
        tables (dict[str, pd.DataFrame]): Diccionario con tablas raw.

    Returns:
        dict[str, pd.DataFrame]: Diccionario con tablas derivados.
    """
    if "clientes" not in tables or "proveedores" not in tables or "vendedores" not in tables:
        missing = [k for k in ("clientes", "proveedores", "vendedores") if k not in tables]
        raise KeyError(f"Faltan tablas en `tables`: {missing}")

    df_clientes = tables["clientes"].copy()
    df_prov = tables["proveedores"].copy()
    df_vend = tables["vendedores"].copy()

    # -----------------------------
    # helpers
    # -----------------------------
    def _norm(s: pd.Series) -> pd.Series:
        return (
            s.astype("string")
             .str.strip()
             .str.casefold()
             .replace({"": pd.NA})
        )

    def _src_key(source: str, ids: pd.Series) -> pd.Series:
        return source + ":" + ids.astype("Int64").astype("string")

    def _pick_first_notna(g: pd.Series) -> object:
        s = g.dropna()
        return s.iloc[0] if len(s) else pd.NA

    def _to_bool(s: pd.Series, default: bool = False) -> pd.Series:
        # asume que ya viene boolean nullable; si viene raro, lo fuerza suave
        out = s.astype("boolean")
        return out.fillna(default)

    # -----------------------------
    # 1) Armar staging unificado (sin PK nueva aún)
    # -----------------------------
    def _prep_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
        out = pd.DataFrame({
            "source": source,
            "source_id": df["id"].astype("Int64"),
            "rut": df["rut"] if "rut" in df.columns else pd.Series([pd.NA] * len(df), dtype="string"),
            "nombre": df["nombre"] if "nombre" in df.columns else pd.Series([pd.NA] * len(df), dtype="string"),
            "alias": df["alias"] if "alias" in df.columns else pd.Series([pd.NA] * len(df), dtype="string"),
            "telefono": df["telefono"] if "telefono" in df.columns else pd.Series([pd.NA] * len(df), dtype="string"),
            "direccion": df["direccion"] if "direccion" in df.columns else pd.Series([pd.NA] * len(df), dtype="string"),
            "is_active": df["is_active"] if "is_active" in df.columns else pd.Series([True] * len(df), dtype="boolean"),
            "created_at": df["created_at"] if "created_at" in df.columns else pd.Series([pd.NA] * len(df)),
            "updated_at": df["updated_at"] if "updated_at" in df.columns else pd.Series([pd.NA] * len(df)),
        })
        out["id_old_key"] = _src_key(source, out["source_id"])
        return out

    st = pd.concat(
        [
            _prep_source(df_clientes, "clientes"),
            _prep_source(df_prov, "proveedores"),
            _prep_source(df_vend, "vendedores"),
        ],
        ignore_index=True,
    )

    # -----------------------------
    # 2) Dedupe (para respetar UNIQUE(rut) en terceros)
    #     - Si hay rut => dedupe por rut normalizado
    #     - Si no hay rut => dedupe por nombre+telefono normalizado (simple)
    # -----------------------------
    rut_key = _norm(st["rut"])
    name_key = _norm(st["nombre"])
    tel_key = _norm(st["telefono"])

    st["dedupe_key"] = rut_key.where(rut_key.notna(), "name:" + name_key.fillna("") + "|tel:" + tel_key.fillna(""))

    # agregación (primero no-nulo / min / max)
    g = st.groupby("dedupe_key", dropna=False)

    terceros = pd.DataFrame({
        "rut": g["rut"].apply(_pick_first_notna).astype("string"),
        "nombre": g["nombre"].apply(_pick_first_notna).astype("string"),
        "alias": g["alias"].apply(_pick_first_notna).astype("string"),
        "telefono": g["telefono"].apply(_pick_first_notna).astype("string"),
        "direccion": g["direccion"].apply(_pick_first_notna).astype("string"),
        "is_active": g["is_active"].max().astype("boolean"),
        "created_at": g["created_at"].min(),
        "updated_at": g["updated_at"].max(),
        # temporal para mapear luego (puede tener varios)
        "id_old": g["id_old_key"].apply(lambda s: "|".join(sorted(set(s.dropna().astype(str).tolist())))).astype("string"),
    }).reset_index(drop=True)

    # PK nueva incremental
    terceros.insert(0, "id", pd.Series(range(1, len(terceros) + 1), dtype="Int64"))

    # -----------------------------
    # 3) Mapping id_old_key -> terceros.id (para construir roles y para tus FKs)
    # -----------------------------
    key_to_tercero_id = (
        pd.DataFrame({"tercero_id": terceros["id"], "id_old": terceros["id_old"]})
        .assign(id_old=lambda d: d["id_old"].astype("string").str.split("|"))
        .explode("id_old")
        .dropna(subset=["id_old"])
        .set_index("id_old")["tercero_id"]
    )

    def _map_tercero_id(source: str, s_ids: pd.Series) -> pd.Series:
        keys = _src_key(source, s_ids.astype("Int64"))
        return keys.map(key_to_tercero_id).astype("Int64")

    # -----------------------------
    # 4) Roles
    # -----------------------------
    # terceros_cliente
    terceros_cliente = pd.DataFrame({
        "tercero_id": _map_tercero_id("clientes", df_clientes["id"]),
        # si ya existe tipo_id, úsalo; si no, deja NA y conserva `tipo` temporal (para mapear a DIM_TIPO_CLIENTE)
        "tipo_id": (df_clientes["tipo_id"].astype("Int64") if "tipo_id" in df_clientes.columns else pd.Series([pd.NA] * len(df_clientes), dtype="Int64")),
        "vendedor_id": (
            _map_tercero_id("vendedores", df_clientes["vendedor_id"])
            if "vendedor_id" in df_clientes.columns
            else pd.Series([pd.NA] * len(df_clientes), dtype="Int64")
        ),
        "factura": _to_bool(df_clientes["factura"] if "factura" in df_clientes.columns else pd.Series([pd.NA] * len(df_clientes))),
        "factura_despacho": _to_bool(df_clientes["factura_despacho"] if "factura_despacho" in df_clientes.columns else pd.Series([pd.NA] * len(df_clientes))),
        "created_at": df_clientes["created_at"] if "created_at" in df_clientes.columns else pd.Series([pd.NA] * len(df_clientes)),
        "updated_at": df_clientes["updated_at"] if "updated_at" in df_clientes.columns else pd.Series([pd.NA] * len(df_clientes)),
    })

    # columna temporal útil (si hoy aún tienes "tipo" string)
    if "tipo" in df_clientes.columns and "tipo_id" not in df_clientes.columns:
        terceros_cliente["tipo"] = df_clientes["tipo"].astype("string")

    terceros_cliente = terceros_cliente.dropna(subset=["tercero_id"]).drop_duplicates(subset=["tercero_id"])
    terceros_cliente["tercero_id"] = terceros_cliente["tercero_id"].astype("Int64")

    # terceros_proveedor
    terceros_proveedor = pd.DataFrame({
        "tercero_id": _map_tercero_id("proveedores", df_prov["id"]),
        "created_at": df_prov["created_at"] if "created_at" in df_prov.columns else pd.Series([pd.NA] * len(df_prov)),
        "updated_at": df_prov["updated_at"] if "updated_at" in df_prov.columns else pd.Series([pd.NA] * len(df_prov)),
    })
    terceros_proveedor = terceros_proveedor.dropna(subset=["tercero_id"]).drop_duplicates(subset=["tercero_id"])
    terceros_proveedor["tercero_id"] = terceros_proveedor["tercero_id"].astype("Int64")

    # terceros_vendedor
    terceros_vendedor = pd.DataFrame({
        "tercero_id": _map_tercero_id("vendedores", df_vend["id"]),
        "modelo_comercial": df_vend["modelo_comercial"].astype("string") if "modelo_comercial" in df_vend.columns else pd.Series([pd.NA] * len(df_vend), dtype="string"),
        "comision": df_vend["comision"].astype("Float64") if "comision" in df_vend.columns else pd.Series([pd.NA] * len(df_vend), dtype="Float64"),
        "is_active": _to_bool(df_vend["is_active"] if "is_active" in df_vend.columns else pd.Series([pd.NA] * len(df_vend))),
        "created_at": df_vend["created_at"] if "created_at" in df_vend.columns else pd.Series([pd.NA] * len(df_vend)),
        "updated_at": df_vend["updated_at"] if "updated_at" in df_vend.columns else pd.Series([pd.NA] * len(df_vend)),
    })
    terceros_vendedor = terceros_vendedor.dropna(subset=["tercero_id"]).drop_duplicates(subset=["tercero_id"])
    terceros_vendedor["tercero_id"] = terceros_vendedor["tercero_id"].astype("Int64")

    return {
        "terceros": terceros,
        "terceros_cliente": terceros_cliente,
        "terceros_proveedor": terceros_proveedor,
        "terceros_vendedor": terceros_vendedor,
    }
