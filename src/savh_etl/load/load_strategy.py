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
        TableSpec("products", df_key="productos"),
        TableSpec("codes", df_key="codigos"),
        TableSpec("dim_type_assets", mode=LoadMode.SKIP),

        # 📦 dimensiones que dependen de productos / otras dims
        TableSpec("dim_conventions", df_key="dim_convenciones"),
        TableSpec("dim_varieties", df_key="dim_variedades", depends_on=("products",)),
        TableSpec("dim_categories", df_key="dim_categorias", depends_on=("products",)),
        TableSpec("dim_calibers", df_key="dim_calibres", depends_on=("products", "dim_conventions")),
        TableSpec("dim_payment_methods", df_key="dim_medios_pago"),
        TableSpec("dim_expense_groups", df_key="dim_grupos_egresos"),
        # En sheets viene como "CAT_TIPOS_EGRESO" (o legacy "CAT_TIPOS_EGRESOS")
        TableSpec(
            "dim_expense_types",
            df_key="dim_tipos_egreso",
            depends_on=("dim_expense_groups",),
        ),
        TableSpec("dim_commercial_models", df_key="dim_modelos_comerciales"),
        TableSpec("dim_salesperson_contract_types", df_key="dim_tipo_contrato_vendedor"),
        TableSpec("dim_salesperson_commission_bases", df_key="dim_base_comision_vendedor"),
        TableSpec("dim_economic_types", df_key="dim_tipos_economicos"),
        TableSpec("dim_shipping_statuses", df_key="dim_estados_despacho"),
        TableSpec("dim_sales_statuses", df_key="dim_estados_venta"),
        TableSpec("dim_sales_billing_statuses", df_key="dim_estados_venta_facturacion"),
        TableSpec("dim_sales_payment_statuses", df_key="dim_estados_venta_pago"),
        TableSpec("dim_purchase_statuses", df_key="dim_estados_compra"),
        TableSpec("dim_customer_types", df_key="dim_tipos_cliente"),
        TableSpec("dim_order_statuses", df_key="dim_estados_pedido"),
        TableSpec("dim_sale_types", df_key="dim_tipos_venta"),
        TableSpec("dim_entity_type"),
        TableSpec("dim_event_type"),

        # 👥 terceros + roles
        TableSpec("parties", df_key="terceros"),
        TableSpec("parties_salesperson", df_key="terceros_vendedor", depends_on=("parties", "dim_commercial_models")),
        TableSpec(
            "salesperson_contracts",
            df_key="contratos_vendedor",
            depends_on=(
                "parties_salesperson",
                "dim_salesperson_contract_types",
                "dim_salesperson_commission_bases",
            ),
        ),
        TableSpec(
            "parties_customer",
            df_key="terceros_cliente",
            depends_on=("parties", "dim_customer_types", "parties_salesperson"),
        ),
        TableSpec("parties_supplier", df_key="terceros_proveedor", depends_on=("parties",)),
        TableSpec("parties_employee", df_key="terceros_trabajador", depends_on=("parties",)),
        TableSpec("parties_recipient", df_key="terceros_destinatario", depends_on=("parties", "parties_customer")),
        TableSpec("assets", df_key="assets", depends_on=("dim_type_assets",)),
        TableSpec("assets_vehicles", df_key="assets_vehicles", depends_on=("assets",)),

        # 🧾 pedidos/ventas
        TableSpec(
            "orders",
            df_key="pedidos",
            depends_on=("parties_customer", "parties_recipient", "dim_order_statuses", "app_users"),
        ),
        TableSpec("order_items", df_key="pedidos_items", depends_on=("orders", "products", "dim_varieties", "dim_categories", "dim_calibers", "codes", "app_users")),
        TableSpec("deliveries", df_key="entregas", depends_on=("orders", "app_users")),

        TableSpec("sales", df_key="ventas", depends_on=("orders", "parties_customer", "parties_recipient",
                                      "dim_sale_types", "dim_sales_statuses", "dim_shipping_statuses",
                                      "dim_sales_billing_statuses", "dim_sales_payment_statuses", "app_users")),
        TableSpec("sale_items", df_key="ventas_items", depends_on=("sales", "products", "dim_varieties", "dim_categories", "dim_calibers", "codes", "app_users")),

        # 💰 pagos
        TableSpec("payments", df_key="pagos", depends_on=("parties_customer", "dim_payment_methods", "app_users")),

        # 🧾 compras / ingresos / mermas
        TableSpec("product_purchases", df_key="compras_producto", depends_on=("codes", "parties_supplier", "dim_purchase_statuses", "app_users")),
        TableSpec("product_purchase_items", df_key="compras_producto_items", depends_on=("product_purchases", "products", "dim_varieties", "dim_categories", "dim_calibers", "app_users")),

        TableSpec("product_receipts", df_key="ingresos_producto", depends_on=("app_users",)),
        TableSpec("product_receipt_items", df_key="ingresos_producto_items", depends_on=("product_receipts", "codes", "products", "dim_varieties", "dim_categories", "dim_calibers", "app_users")),

        TableSpec("product_losses", df_key="mermas", depends_on=("codes", "products", "dim_varieties", "dim_categories", "dim_calibers", "app_users")),

        # 🧾 egresos + N-M
        TableSpec("expenses", df_key="egresos", depends_on=("dim_expense_types", "dim_economic_types", "dim_payment_methods",
                                        "assets", "parties",
                                        "app_users")),
        TableSpec("purchase_payment_applications", df_key="pagos_compra", depends_on=("product_purchases", "expenses", "app_users")),

        # 🧬 eventos/auditoría (si los vas a cargar desde sheets)
        TableSpec("ingest_events"),
        TableSpec("entity_events", depends_on=("ingest_events", "dim_entity_type", "dim_event_type", "app_users")),
        TableSpec("audit_log", depends_on=("ingest_events", "app_users")),
    ]

    return LoadPlan(tuple(specs))
