-- 05_indexes.sql
-- Índices (performance) y únicos (integridad) que no están embebidos inline en CREATE TABLE.
--
-- Nota:
--   - Postgres NO crea índices automáticamente para Foreign Keys. Para joins y borrados/updates
--     eficientes, es estándar indexar las columnas FK del lado "muchos".

-- =====================
-- ✅ Únicos (integridad)
-- =====================

-- Dims compuestas
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_variedades_producto_variedad
  ON core.dim_varieties (producto_id, variedad);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_categorias_producto_categoria
  ON core.dim_categories (producto_id, categoria);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_calibres_producto_calibre_convencion
  ON core.dim_calibers (producto_id, calibre, convencion_id);

-- Dims simples que deberían ser únicas (si la convención se usa como catálogo)
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_convenciones_convencion
  ON core.dim_conventions (convencion);

CREATE UNIQUE INDEX IF NOT EXISTS ux_pagos_compra_compra_egreso
  ON core.purchase_payment_applications (compra_id, egreso_id);

CREATE INDEX IF NOT EXISTS ix_assets_tipo_activo_id
  ON core.assets (tipo_activo_id);

CREATE INDEX IF NOT EXISTS ix_assets_vehicles_patente
  ON core.assets_vehicles (patente);

-- Idempotencia / trazabilidad
CREATE UNIQUE INDEX IF NOT EXISTS ux_ingest_events_idempotency_key
  ON ingest.ingest_events (idempotency_key);

CREATE UNIQUE INDEX IF NOT EXISTS ux_ingest_events_source_external
  ON ingest.ingest_events (source_system, external_id);

CREATE INDEX IF NOT EXISTS ix_entity_events_entity
  ON ingest.entity_events (entity_type_id, entity_id, event_at DESC);


-- =====================
-- 🚀 Índices recomendados (performance)
-- =====================

-- --- PEDIDOS ---
CREATE INDEX IF NOT EXISTS ix_pedidos_fecha           ON core.orders (fecha);
CREATE INDEX IF NOT EXISTS ix_pedidos_cliente_id      ON core.orders (cliente_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_destinatario_id ON core.orders (destinatario_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_estado_id       ON core.orders (estado_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_ingest_event_id ON core.orders (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_pedidos_items_pedido_id     ON core.order_items (pedido_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_items_producto_id   ON core.order_items (producto_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_items_variedad_id   ON core.order_items (variedad_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_items_categoria_id  ON core.order_items (categoria_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_items_calibre_id    ON core.order_items (calibre_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_items_codigo_id     ON core.order_items (codigo_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_items_ingest_event_id ON core.order_items (ingest_event_id);

-- ENTREGAS: pedido_id ya es UNIQUE en la tabla (ya existe índice implícito)

-- --- VENTAS ---
CREATE INDEX IF NOT EXISTS ix_ventas_fecha                ON core.sales (fecha);
CREATE INDEX IF NOT EXISTS ix_ventas_cliente_id           ON core.sales (cliente_id);
CREATE INDEX IF NOT EXISTS ix_ventas_destinatario_id      ON core.sales (destinatario_id);
CREATE INDEX IF NOT EXISTS ix_ventas_tipo_id              ON core.sales (tipo_id);
CREATE INDEX IF NOT EXISTS ix_ventas_estado_venta_id      ON core.sales (estado_venta_id);
CREATE INDEX IF NOT EXISTS ix_ventas_estado_despacho_id   ON core.sales (estado_despacho_id);
CREATE INDEX IF NOT EXISTS ix_ventas_estado_facturacion_id ON core.sales (estado_facturacion_id);
CREATE INDEX IF NOT EXISTS ix_ventas_estado_pago_id       ON core.sales (estado_pago_id);
CREATE INDEX IF NOT EXISTS ix_ventas_ingest_event_id      ON core.sales (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_ventas_items_venta_id       ON core.sale_items (venta_id);
CREATE INDEX IF NOT EXISTS ix_ventas_items_producto_id    ON core.sale_items (producto_id);
CREATE INDEX IF NOT EXISTS ix_ventas_items_variedad_id    ON core.sale_items (variedad_id);
CREATE INDEX IF NOT EXISTS ix_ventas_items_categoria_id   ON core.sale_items (categoria_id);
CREATE INDEX IF NOT EXISTS ix_ventas_items_calibre_id     ON core.sale_items (calibre_id);
CREATE INDEX IF NOT EXISTS ix_ventas_items_codigo_id      ON core.sale_items (codigo_id);
CREATE INDEX IF NOT EXISTS ix_ventas_items_ingest_event_id ON core.sale_items (ingest_event_id);

-- --- PAGOS ---
CREATE INDEX IF NOT EXISTS ix_pagos_fecha            ON core.payments (fecha);
CREATE INDEX IF NOT EXISTS ix_pagos_cliente_id       ON core.payments (cliente_id);
CREATE INDEX IF NOT EXISTS ix_pagos_medio_pago_id    ON core.payments (medio_pago_id);
CREATE INDEX IF NOT EXISTS ix_pagos_ingest_event_id  ON core.payments (ingest_event_id);

-- --- COMPRAS / INGRESOS / MERMAS ---
CREATE INDEX IF NOT EXISTS ix_compras_producto_fecha        ON core.product_purchases (fecha);
CREATE INDEX IF NOT EXISTS ix_compras_producto_codigo_id    ON core.product_purchases (codigo_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_proveedor_id ON core.product_purchases (proveedor_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_estado_compra_id ON core.product_purchases (estado_compra_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_ingest_event_id ON core.product_purchases (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_compras_producto_items_compra_id   ON core.product_purchase_items (compra_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_items_producto_id ON core.product_purchase_items (producto_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_items_variedad_id ON core.product_purchase_items (variedad_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_items_categoria_id ON core.product_purchase_items (categoria_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_items_calibre_id  ON core.product_purchase_items (calibre_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_items_ingest_event_id ON core.product_purchase_items (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_ingresos_producto_fecha       ON core.product_receipts (fecha);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_ingest_event_id ON core.product_receipts (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_ingreso_id ON core.product_receipt_items (ingreso_id);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_codigo_id  ON core.product_receipt_items (codigo_id);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_producto_id ON core.product_receipt_items (producto_id);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_variedad_id ON core.product_receipt_items (variedad_id);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_categoria_id ON core.product_receipt_items (categoria_id);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_calibre_id  ON core.product_receipt_items (calibre_id);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_ingest_event_id ON core.product_receipt_items (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_mermas_fecha            ON core.product_losses (fecha);
CREATE INDEX IF NOT EXISTS ix_mermas_codigo_id        ON core.product_losses (codigo_id);
CREATE INDEX IF NOT EXISTS ix_mermas_producto_id      ON core.product_losses (producto_id);
CREATE INDEX IF NOT EXISTS ix_mermas_ingest_event_id  ON core.product_losses (ingest_event_id);

-- --- EGRESOS ---
CREATE INDEX IF NOT EXISTS ix_egresos_fecha           ON core.expenses (fecha);
CREATE INDEX IF NOT EXISTS ix_egresos_tipo_egreso_id  ON core.expenses (tipo_egreso_id);
CREATE INDEX IF NOT EXISTS ix_egresos_tipo_economico_id ON core.expenses (tipo_economico_id);
CREATE INDEX IF NOT EXISTS ix_egresos_medio_pago_id   ON core.expenses (medio_pago_id);
CREATE INDEX IF NOT EXISTS ix_egresos_activo_id       ON core.expenses (activo_id);
CREATE INDEX IF NOT EXISTS ix_egresos_actor_id        ON core.expenses (actor_id);
CREATE INDEX IF NOT EXISTS ix_egresos_ingest_event_id ON core.expenses (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_pagos_compra_compra_id  ON core.purchase_payment_applications (compra_id);
CREATE INDEX IF NOT EXISTS ix_pagos_compra_egreso_id  ON core.purchase_payment_applications (egreso_id);
CREATE INDEX IF NOT EXISTS ix_pagos_compra_ingest_event_id ON core.purchase_payment_applications (ingest_event_id);

-- --- ROLES TERCEROS ---
CREATE INDEX IF NOT EXISTS ix_terceros_destinatario_cliente_id ON core.parties_recipient (cliente_id);
CREATE INDEX IF NOT EXISTS ix_salesperson_contracts_vendedor_id ON core.salesperson_contracts (vendedor_id);
CREATE INDEX IF NOT EXISTS ix_salesperson_contracts_tipo_id ON core.salesperson_contracts (tipo_id);
CREATE INDEX IF NOT EXISTS ix_salesperson_contracts_base_id ON core.salesperson_contracts (base_id);

-- --- EVENTOS / AUDITORÍA ---
CREATE INDEX IF NOT EXISTS ix_entity_events_ingest_event_id ON ingest.entity_events (ingest_event_id);
CREATE INDEX IF NOT EXISTS ix_entity_events_event_at        ON ingest.entity_events (event_at DESC);

CREATE INDEX IF NOT EXISTS ix_audit_log_occurred_at          ON audit.audit_log (occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_audit_log_table_occurred_at    ON audit.audit_log (table_name, occurred_at DESC);

-- --- SOFT DELETE (is_deleted) ---
CREATE INDEX IF NOT EXISTS ix_pedidos_fecha_alive ON core.orders (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_entregas_fecha_alive ON core.deliveries (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_ventas_fecha_alive ON core.sales (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_pagos_fecha_alive ON core.payments (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_compras_producto_fecha_alive ON core.product_purchases (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_fecha_alive ON core.product_receipts (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_mermas_fecha_alive ON core.product_losses (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_egresos_fecha_alive ON core.expenses (fecha) WHERE deleted_at IS NULL;
