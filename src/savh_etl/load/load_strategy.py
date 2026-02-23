# load/load_strategy.py
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Iterable

import pandas as pd


# =========================
# Tipos base
# =========================

class LoadMode(str, Enum):
    """Cómo quieres cargar una tabla."""
    INSERT = "insert"          # insertar (ideal one-shot)
    SKIP = "skip"              # no cargarla (por ahora)


@dataclass(frozen=True)
class TableSpec:
    """
    Define UNA tabla a cargar:
    - name: nombre real en Postgres (ya en minúscula)
    - df_key: llave del dict_df (si coincide con name, déjalo igual)
    - depends_on: para ordenar la carga (FKs)
    - preprocess: hook opcional para tweaks de DF antes de cargar
    - mode: INSERT / SKIP
    """
    name: str
    df_key: str | None = None
    depends_on: tuple[str, ...] = ()
    preprocess: Callable[[pd.DataFrame], pd.DataFrame] | None = None
    mode: LoadMode = LoadMode.INSERT

    def key(self) -> str:
        """Obtiene la llave del dict_df asociada a esta tabla.

        Returns:
            str: Llave a usar en dict_df (df_key si existe; si no, name).
        """
        return self.df_key or self.name


class LoadPlanError(RuntimeError):
    """Error al construir o validar un LoadPlan."""
    pass


# =========================
# Plan + ordenamiento
# =========================

def _toposort(specs: Iterable[TableSpec]) -> list[TableSpec]:
    """Ordena TableSpec por dependencias usando el algoritmo de Kahn.

    Args:
        specs (Iterable[TableSpec]): Especificaciones de tabla.

    Returns:
        list[TableSpec]: Lista ordenada por dependencias.

    Raises:
        LoadPlanError: Si falta una dependencia o hay un ciclo.
    """
    specs_list = list(specs)
    by_name = {s.name: s for s in specs_list}

    # valida deps existentes
    for s in specs_list:
        for d in s.depends_on:
            if d not in by_name:
                raise LoadPlanError(f"Dependencia '{d}' no existe (requerida por '{s.name}').")

    incoming = {s.name: set(s.depends_on) for s in specs_list}
    ready = [name for name, deps in incoming.items() if not deps]
    ordered: list[TableSpec] = []

    while ready:
        n = ready.pop()
        ordered.append(by_name[n])

        for m in incoming:
            if n in incoming[m]:
                incoming[m].remove(n)
                if not incoming[m]:
                    ready.append(m)

    if len(ordered) != len(specs_list):
        # lo que quedó con deps => ciclo
        cyclic = {k: v for k, v in incoming.items() if v}
        raise LoadPlanError(f"Ciclo o deps irresolubles en plan: {cyclic}")

    return ordered


@dataclass(frozen=True)
class LoadPlan:
    """
    Plan final:
    - specs: definición completa
    - order: orden de carga (resuelto por toposort)
    """
    specs: tuple[TableSpec, ...]
    order: tuple[TableSpec, ...] = field(init=False)

    def __post_init__(self):
        """Resuelve el orden de carga a partir de `specs`."""
        object.__setattr__(self, "order", tuple(_toposort(self.specs)))

    def table_names_in_order(self) -> list[str]:
        """Lista nombres de tabla en orden de carga.

        Returns:
            list[str]: Nombres de tabla para specs con modo INSERT.
        """
        return [s.name for s in self.order if s.mode == LoadMode.INSERT]

    def keys_in_order(self) -> list[str]:
        """Lista llaves de dict_df en orden de carga.

        Returns:
            list[str]: Llaves de dict_df para specs con modo INSERT.
        """
        return [s.key() for s in self.order if s.mode == LoadMode.INSERT]

    def validate_inputs(self, dict_df: dict[str, pd.DataFrame]) -> None:
        """Valida que dict_df tenga los DataFrames requeridos.

        Args:
            dict_df (dict[str, pd.DataFrame]): Mapa de llaves a DataFrames extraídos.

        Raises:
            LoadPlanError: Si faltan DataFrames para tablas en modo INSERT.
        """
        missing = [s.key() for s in self.order if s.mode == LoadMode.INSERT and s.key() not in dict_df]
        if missing:
            raise LoadPlanError(f"Faltan DataFrames en dict_df para: {missing}")


# =========================
# 🗂️ Plan SAVH (one-shot)
# =========================

def build_savh_load_plan() -> LoadPlan:
    """Construye el plan de carga recomendado para SAVH.

    Orden sugerido:
      - tablas sin dependencias
      - dimensiones y master
      - hechos, items y N-M
      - eventos/auditoría (si aplica)

    Returns:
        LoadPlan: Plan con el orden de carga resuelto.
    """
    specs = [

        # 👤 usuarios
        TableSpec("app_users"),

        # 📦 master base
        TableSpec("productos"),
        TableSpec("codigos"),

        # 📦 dimensiones que dependen de productos / otras dims
        TableSpec("dim_convenciones"),
        TableSpec("dim_variedades", depends_on=("productos",)),
        TableSpec("dim_categorias", depends_on=("productos",)),
        TableSpec("dim_calibres", depends_on=("productos", "dim_convenciones")),
        TableSpec("dim_medios_pago"),
        TableSpec("dim_grupos_egresos"),
        # En sheets suele venir como "CAT_TIPOS_EGRESOS" => "dim_tipos_egresos"
        TableSpec(
            "dim_tipos_egreso",
            df_key="dim_tipos_egresos",
            depends_on=("dim_grupos_egresos",),
        ),
        TableSpec("dim_modelos_comerciales"),
        TableSpec("dim_tipos_economicos"),
        TableSpec("dim_estados_despacho"),
        TableSpec("dim_estados_venta"),
        TableSpec("dim_estados_venta_facturacion"),
        TableSpec("dim_estados_venta_pago"),
        TableSpec("dim_estados_compra"),
        TableSpec("dim_tipos_clientes"),
        TableSpec("dim_estados_pedido"),
        TableSpec("dim_tipos_venta"),
        TableSpec("dim_entity_type"),
        TableSpec("dim_event_type"),

        # 👥 terceros + roles
        TableSpec("terceros"),
        TableSpec("terceros_vendedor", depends_on=("terceros", "dim_modelos_comerciales")),
        TableSpec(
            "terceros_cliente",
            depends_on=("terceros", "dim_tipos_clientes", "terceros_vendedor"),
        ),
        TableSpec("terceros_proveedor", depends_on=("terceros",)),
        TableSpec("destinatarios", depends_on=("terceros_cliente",)),
        TableSpec("trabajadores"),
        TableSpec("vehiculos"),

        # 🧾 pedidos/ventas
        TableSpec(
            "pedidos",
            depends_on=("terceros_cliente", "destinatarios", "dim_estados_pedido", "app_users"),
        ),
        TableSpec("pedidos_items", depends_on=("pedidos", "productos", "dim_variedades", "dim_categorias", "dim_calibres", "codigos", "app_users")),
        TableSpec("entregas", depends_on=("pedidos", "app_users")),

        TableSpec("ventas", depends_on=("pedidos", "terceros_cliente", "destinatarios",
                                      "dim_tipos_venta", "dim_estados_venta", "dim_estados_despacho",
                                      "dim_estados_venta_facturacion", "dim_estados_venta_pago", "app_users")),
        TableSpec("ventas_items", depends_on=("ventas", "productos", "dim_variedades", "dim_categorias", "dim_calibres", "codigos", "app_users")),

        # 💰 pagos
        TableSpec("pagos", depends_on=("terceros_cliente", "dim_medios_pago", "app_users")),
        TableSpec("aplicaciones_pago", depends_on=("pagos", "ventas", "app_users")),

        # 🧾 compras / ingresos / mermas
        TableSpec("compras_producto", depends_on=("codigos", "terceros_proveedor", "dim_estados_compra", "app_users")),
        TableSpec("compras_producto_items", depends_on=("compras_producto", "productos", "dim_variedades", "dim_categorias", "dim_calibres", "app_users")),

        TableSpec("ingresos_producto", depends_on=("app_users",)),
        TableSpec("ingresos_producto_items", depends_on=("ingresos_producto", "codigos", "productos", "dim_variedades", "dim_categorias", "dim_calibres", "app_users")),

        TableSpec("mermas", depends_on=("codigos", "productos", "dim_variedades", "dim_categorias", "dim_calibres", "app_users")),

        # 🧾 egresos + N-M
        TableSpec("egresos", depends_on=("dim_tipos_egreso", "dim_tipos_economicos", "dim_medios_pago",
                                        "vehiculos", "trabajadores",
                                        "terceros_vendedor", "terceros_cliente", "terceros_proveedor",
                                        "app_users")),
        TableSpec("pagos_compra", depends_on=("compras_producto", "egresos", "app_users")),

        # 🧬 eventos/auditoría (si los vas a cargar desde sheets)
        TableSpec("ingest_events"),
        TableSpec("entity_events", depends_on=("ingest_events", "dim_entity_type", "dim_event_type", "app_users")),
        TableSpec("audit_log", depends_on=("ingest_events", "app_users")),
    ]

    return LoadPlan(tuple(specs))
