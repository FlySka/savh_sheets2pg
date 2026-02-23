from __future__ import annotations

import pandas as pd

from savh_etl.transform.pipeline import TablePipeline
from savh_etl.transform.tables.all_tables import get_all_tables_pipeline
from savh_etl.transform.tables.clientes import get_clientes_pipeline
from savh_etl.transform.tables.codigos import get_codigos_pipeline
from savh_etl.transform.tables.compras_producto import get_compras_producto_pipeline
from savh_etl.transform.tables.compras_producto_items import get_compras_producto_items_pipeline
from savh_etl.transform.tables.destinatarios import get_destinatarios_pipeline
from savh_etl.transform.tables.dim_calibres import get_dim_calibres_pipeline
from savh_etl.transform.tables.dim_categorias import get_dim_categorias_pipeline
from savh_etl.transform.tables.dim_convenciones import get_dim_convenciones_pipeline
from savh_etl.transform.tables.dim_estados_compra import get_dim_estados_compra_pipeline
from savh_etl.transform.tables.dim_estados_despacho import get_dim_estados_despacho_pipeline
from savh_etl.transform.tables.dim_estados_pedido import get_dim_estados_pedido_pipeline
from savh_etl.transform.tables.dim_estados_venta import get_dim_estados_venta_pipeline
from savh_etl.transform.tables.dim_estados_venta_facturacion import get_dim_estados_venta_facturacion_pipeline
from savh_etl.transform.tables.dim_estados_venta_pago import get_dim_estados_venta_pago_pipeline
from savh_etl.transform.tables.dim_grupos_egresos import get_dim_grupos_egresos_pipeline
from savh_etl.transform.tables.dim_medios_pago import get_dim_medios_pago_pipeline
from savh_etl.transform.tables.dim_modelos_comerciales import get_dim_modelos_comerciales_pipeline
from savh_etl.transform.tables.dim_tipos_economicos import get_dim_tipos_economicos_pipeline
from savh_etl.transform.tables.dim_tipos_egreso import get_dim_tipos_egreso_pipeline
from savh_etl.transform.tables.dim_tipos_venta import get_dim_tipos_venta_pipeline
from savh_etl.transform.tables.dim_variedades import get_dim_variedades_pipeline
from savh_etl.transform.tables.egresos import get_egresos_pipeline
from savh_etl.transform.tables.entregas import get_entregas_pipeline
from savh_etl.transform.tables.ingresos_producto import get_ingresos_producto_pipeline
from savh_etl.transform.tables.ingresos_producto_items import get_ingresos_producto_items_pipeline
from savh_etl.transform.tables.mermas import get_mermas_pipeline
from savh_etl.transform.tables.pagos import get_pagos_pipeline
from savh_etl.transform.tables.pagos_compra import get_pagos_compra_pipeline
from savh_etl.transform.tables.pedidos import get_pedidos_pipeline
from savh_etl.transform.tables.pedidos_items import get_pedidos_items_pipeline
from savh_etl.transform.tables.productos import get_productos_pipeline
from savh_etl.transform.tables.proveedores import get_proveedores_pipeline
from savh_etl.transform.tables.trabajadores import get_trabajadores_pipeline
from savh_etl.transform.tables.vehiculos import get_vehiculos_pipeline
from savh_etl.transform.tables.vendedores import get_vendedores_pipeline
from savh_etl.transform.tables.ventas import get_compras_producto_pipeline as get_ventas_pipeline
from savh_etl.transform.tables.ventas_items import get_ventas_items_pipeline
from savh_etl.utils.logging import get_logger
log = get_logger(__name__)


def get_registry(tables: dict[str, pd.DataFrame]) -> dict[str, TablePipeline]:
    """Retorna pipelines por tabla (nombre SQL normalizado).
    
    Returns:
        Dict tabla -> TablePipeline.
    """
    return {
        "all_tables": get_all_tables_pipeline(),
        "clientes": get_clientes_pipeline(tables),
        "codigos": get_codigos_pipeline(),
        "compras_producto": get_compras_producto_pipeline(tables),
        "compras_producto_items": get_compras_producto_items_pipeline(tables),
        "destinatarios": get_destinatarios_pipeline(tables),
        "dim_calibres": get_dim_calibres_pipeline(tables),
        "dim_categorias": get_dim_categorias_pipeline(tables),
        "dim_convenciones": get_dim_convenciones_pipeline(),
        "dim_estados_compra": get_dim_estados_compra_pipeline(),
        "dim_estados_despacho": get_dim_estados_despacho_pipeline(),
        "dim_estados_pedido": get_dim_estados_pedido_pipeline(),
        "dim_estados_venta": get_dim_estados_venta_pipeline(),
        "dim_estados_venta_facturacion": get_dim_estados_venta_facturacion_pipeline(),
        "dim_estados_venta_pago": get_dim_estados_venta_pago_pipeline(),
        "dim_grupos_egresos": get_dim_grupos_egresos_pipeline(),
        "dim_medios_pago": get_dim_medios_pago_pipeline(),
        "dim_modelos_comerciales": get_dim_modelos_comerciales_pipeline(),
        "dim_tipos_economicos": get_dim_tipos_economicos_pipeline(),
        "dim_tipos_egreso": get_dim_tipos_egreso_pipeline(tables),
        "dim_tipos_venta": get_dim_tipos_venta_pipeline(),
        "dim_variedades": get_dim_variedades_pipeline(tables),
        "egresos": get_egresos_pipeline(tables),
        "entregas": get_entregas_pipeline(),
        "ingresos_producto": get_ingresos_producto_pipeline(),
        "ingresos_producto_items": get_ingresos_producto_items_pipeline(tables),
        "mermas": get_mermas_pipeline(tables),
        "pagos": get_pagos_pipeline(tables),
        "pagos_compra": get_pagos_compra_pipeline(),
        "pedidos": get_pedidos_pipeline(tables),
        "pedidos_items": get_pedidos_items_pipeline(tables),
        "productos": get_productos_pipeline(),
        "proveedores": get_proveedores_pipeline(),
        "trabajadores": get_trabajadores_pipeline(),
        "vehiculos": get_vehiculos_pipeline(),
        "vendedores": get_vendedores_pipeline(),
        "ventas": get_ventas_pipeline(tables),
        "ventas_items": get_ventas_items_pipeline(tables),
    }
