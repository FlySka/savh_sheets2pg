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
      - terceros_trabajador
      - terceros_destinatario

    Reglas clave:
      - Genera PK incremental `terceros.id`.
      - Mantiene trazabilidad con `id_old` (ej: "clientes:12|proveedores:3").
      - No elimina tablas raw

    Args:   
        tables (dict[str, pd.DataFrame]): Diccionario con tablas raw.

    Returns:
        dict[str, pd.DataFrame]: Diccionario con tablas derivados.
    """
    required = ("clientes", "proveedores", "vendedores", "trabajadores", "destinatarios")
    if any(k not in tables for k in required):
        missing = [k for k in required if k not in tables]
        raise KeyError(f"Faltan tablas en `tables`: {missing}")

    df_clientes = tables["clientes"].copy()
    df_prov = tables["proveedores"].copy()
    df_vend = tables["vendedores"].copy()
    df_trab = tables["trabajadores"].copy()
    df_dest = tables["destinatarios"].copy()

    # -----------------------------
    # helpers
    # -----------------------------
    def _norm(s: pd.Series) -> pd.Series:
        return (
            s.astype("string")
             .str.strip()
             .str.replace(r"\s+", " ", regex=True)
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
    def _prep_source(
        df: pd.DataFrame,
        source: str,
        *,
        nombre_override: pd.Series | None = None,
    ) -> pd.DataFrame:
        nombre = (
            nombre_override.astype("string")
            if nombre_override is not None
            else (
                df["nombre"]
                if "nombre" in df.columns
                else pd.Series([pd.NA] * len(df), dtype="string")
            )
        )
        out = pd.DataFrame({
            "source": source,
            "source_id": df["id"].astype("Int64"),
            "rut": df["rut"] if "rut" in df.columns else pd.Series([pd.NA] * len(df), dtype="string"),
            "nombre": nombre,
            "alias": df["alias"] if "alias" in df.columns else pd.Series([pd.NA] * len(df), dtype="string"),
            "telefono": df["telefono"] if "telefono" in df.columns else pd.Series([pd.NA] * len(df), dtype="string"),
            "direccion": df["direccion"] if "direccion" in df.columns else pd.Series([pd.NA] * len(df), dtype="string"),
            "is_active": df["is_active"] if "is_active" in df.columns else pd.Series([True] * len(df), dtype="boolean"),
            "created_at": df["created_at"] if "created_at" in df.columns else pd.Series([pd.NA] * len(df)),
            "updated_at": df["updated_at"] if "updated_at" in df.columns else pd.Series([pd.NA] * len(df)),
        })
        out["id_old_key"] = _src_key(source, out["source_id"])
        return out

    nombre_trab = (
        df_trab["nombre"].astype("string")
        if "nombre" in df_trab.columns
        else pd.Series([pd.NA] * len(df_trab), dtype="string")
    )
    apellido_trab = (
        df_trab["apellido"].astype("string")
        if "apellido" in df_trab.columns
        else pd.Series([pd.NA] * len(df_trab), dtype="string")
    )
    # nombre en terceros para trabajadores: "nombre + apellido" cuando exista.
    nombre_tercero_trab = (nombre_trab.fillna("").str.strip() + " " + apellido_trab.fillna("").str.strip()).str.strip()
    nombre_tercero_trab = nombre_tercero_trab.mask(nombre_tercero_trab == "", pd.NA).astype("string")

    st = pd.concat(
        [
            _prep_source(df_clientes, "clientes"),
            _prep_source(df_prov, "proveedores"),
            _prep_source(df_vend, "vendedores"),
            _prep_source(df_trab, "trabajadores", nombre_override=nombre_tercero_trab),
            _prep_source(df_dest, "destinatarios"),
        ],
        ignore_index=True,
    )

    # -----------------------------
    # 2) Dedupe (regla de negocio actual)
    #     - Si hay nombre => dedupe por nombre normalizado
    #     - Si no hay nombre y hay rut => dedupe por rut normalizado
    #     - Si faltan ambos => clave unica por source/source_id
    # -----------------------------
    rut_key = _norm(st["rut"])
    name_key = _norm(st["nombre"])
    st["dedupe_key"] = "name:" + name_key.fillna("")
    no_name_mask = name_key.isna()
    st.loc[no_name_mask & rut_key.notna(), "dedupe_key"] = "rut:" + rut_key[no_name_mask & rut_key.notna()].fillna("")
    missing_name_mask = no_name_mask & rut_key.isna()
    st.loc[missing_name_mask, "dedupe_key"] = (
        "missing_name:"
        + st.loc[missing_name_mask, "source"].astype("string")
        + ":"
        + st.loc[missing_name_mask, "source_id"].astype("Int64").astype("string")
    )
    if missing_name_mask.any():
        log.warning(
            "Terceros con rut/nombre vacios: %d filas. Se dedupean por source+source_id.",
            int(missing_name_mask.sum()),
        )

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

    # Validacion defensiva: nombres no vacios deben ser unicos tras dedupe.
    nombre_norm = _norm(terceros["nombre"])
    dup_names = nombre_norm[nombre_norm.notna() & nombre_norm.duplicated()].unique().tolist()
    if dup_names:
        examples: list[str] = []
        for name in dup_names[:10]:
            ids = (
                terceros.loc[nombre_norm.eq(name), "id_old"]
                .astype("string")
                .dropna()
                .tolist()
            )
            examples.append(f"{name} -> {ids}")
        raise ValueError(
            "Duplicados en terceros por nombre normalizado. "
            f"Ejemplos: {examples}"
        )

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
        "modelo_comercial_id": (
            df_vend["modelo_comercial_id"].astype("Int64")
            if "modelo_comercial_id" in df_vend.columns
            else pd.Series([pd.NA] * len(df_vend), dtype="Int64")
        ),
        "is_active": _to_bool(df_vend["is_active"] if "is_active" in df_vend.columns else pd.Series([pd.NA] * len(df_vend))),
        "created_at": df_vend["created_at"] if "created_at" in df_vend.columns else pd.Series([pd.NA] * len(df_vend)),
        "updated_at": df_vend["updated_at"] if "updated_at" in df_vend.columns else pd.Series([pd.NA] * len(df_vend)),
    })
    terceros_vendedor = terceros_vendedor.dropna(subset=["tercero_id"]).drop_duplicates(subset=["tercero_id"])
    terceros_vendedor["tercero_id"] = terceros_vendedor["tercero_id"].astype("Int64")

    # terceros_trabajador
    terceros_trabajador = pd.DataFrame({
        "tercero_id": _map_tercero_id("trabajadores", df_trab["id"]),
        "cargo": df_trab["cargo"].astype("string") if "cargo" in df_trab.columns else pd.Series([pd.NA] * len(df_trab), dtype="string"),
        "created_at": df_trab["created_at"] if "created_at" in df_trab.columns else pd.Series([pd.NA] * len(df_trab)),
        "updated_at": df_trab["updated_at"] if "updated_at" in df_trab.columns else pd.Series([pd.NA] * len(df_trab)),
    })
    terceros_trabajador = terceros_trabajador.dropna(subset=["tercero_id"]).drop_duplicates(subset=["tercero_id"])
    terceros_trabajador["tercero_id"] = terceros_trabajador["tercero_id"].astype("Int64")

    # terceros_destinatario
    terceros_destinatario = pd.DataFrame({
        "tercero_id": _map_tercero_id("destinatarios", df_dest["id"]),
        "cliente_id": (
            _map_tercero_id("clientes", df_dest["cliente_id"])
            if "cliente_id" in df_dest.columns
            else pd.Series([pd.NA] * len(df_dest), dtype="Int64")
        ),
        "is_active": _to_bool(df_dest["is_active"], default=True) if "is_active" in df_dest.columns else pd.Series([True] * len(df_dest), dtype="boolean"),
        "created_at": df_dest["created_at"] if "created_at" in df_dest.columns else pd.Series([pd.NA] * len(df_dest)),
        "updated_at": df_dest["updated_at"] if "updated_at" in df_dest.columns else pd.Series([pd.NA] * len(df_dest)),
    })
    terceros_destinatario = terceros_destinatario.dropna(subset=["tercero_id"]).drop_duplicates(subset=["tercero_id"])
    terceros_destinatario["tercero_id"] = terceros_destinatario["tercero_id"].astype("Int64")
    terceros_destinatario["cliente_id"] = terceros_destinatario["cliente_id"].astype("Int64")

    return {
        "terceros": terceros,
        "terceros_cliente": terceros_cliente,
        "terceros_proveedor": terceros_proveedor,
        "terceros_vendedor": terceros_vendedor,
        "terceros_trabajador": terceros_trabajador,
        "terceros_destinatario": terceros_destinatario,
    }


def build_assets_tables(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Construye tablas derivadas de activos desde la tabla vehiculos."""
    if "vehiculos" not in tables:
        raise KeyError("Falta tabla en `tables`: ['vehiculos']")

    df_vehiculos = tables["vehiculos"].copy()
    row_count = len(df_vehiculos)

    def _col(name: str, *, dtype: str, default: object = pd.NA) -> pd.Series:
        if name in df_vehiculos.columns:
            return df_vehiculos[name].astype(dtype)
        return pd.Series([default] * row_count, dtype=dtype)

    assets = pd.DataFrame({
        "id": _col("id", dtype="Int64"),
        "alias": _col("alias", dtype="string"),
        "tipo_activo_id": pd.Series([1] * row_count, dtype="Int64"),
        "is_active": _col("is_active", dtype="boolean", default=True),
    })

    vehicle_type = (
        _col("tipo", dtype="string")
        if "tipo" in df_vehiculos.columns
        else _col("tipo_vehiculo", dtype="string")
    )

    assets_vehicles = pd.DataFrame({
        "activo_id": _col("id", dtype="Int64"),
        "patente": _col("patente", dtype="string"),
        "tipo": vehicle_type,
        "marca": _col("marca", dtype="string"),
        "modelo": _col("modelo", dtype="string"),
        "anio": _col("anio", dtype="Int64"),
    })

    return {
        "assets": assets,
        "assets_vehicles": assets_vehicles,
    }
