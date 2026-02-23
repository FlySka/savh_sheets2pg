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
  ON core.dim_variedades (producto_id, variedad);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_categorias_producto_categoria
  ON core.dim_categorias (producto_id, categoria);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_calibres_producto_calibre_convencion
  ON core.dim_calibres (producto_id, calibre, convencion_id);

-- Dims simples que deberían ser únicas (si la convención se usa como catálogo)
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_convenciones_convencion
  ON core.dim_convenciones (convencion);

-- Unicidad compuesta de negocio
CREATE UNIQUE INDEX IF NOT EXISTS ux_destinatarios_cliente_nombre
  ON core.destinatarios (cliente_id, nombre);

CREATE UNIQUE INDEX IF NOT EXISTS ux_aplicaciones_pago_pago_venta
  ON core.aplicaciones_pago (pago_id, venta_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_pagos_compra_compra_egreso
  ON core.pagos_compra (compra_id, egreso_id);

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
CREATE INDEX IF NOT EXISTS ix_pedidos_fecha           ON core.pedidos (fecha);
CREATE INDEX IF NOT EXISTS ix_pedidos_cliente_id      ON core.pedidos (cliente_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_destinatario_id ON core.pedidos (destinatario_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_estado_id       ON core.pedidos (estado_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_ingest_event_id ON core.pedidos (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_pedidos_items_pedido_id     ON core.pedidos_items (pedido_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_items_producto_id   ON core.pedidos_items (producto_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_items_variedad_id   ON core.pedidos_items (variedad_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_items_categoria_id  ON core.pedidos_items (categoria_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_items_calibre_id    ON core.pedidos_items (calibre_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_items_codigo_id     ON core.pedidos_items (codigo_id);
CREATE INDEX IF NOT EXISTS ix_pedidos_items_ingest_event_id ON core.pedidos_items (ingest_event_id);

-- ENTREGAS: pedido_id ya es UNIQUE en la tabla (ya existe índice implícito)

-- --- VENTAS ---
CREATE INDEX IF NOT EXISTS ix_ventas_fecha                ON core.ventas (fecha);
CREATE INDEX IF NOT EXISTS ix_ventas_cliente_id           ON core.ventas (cliente_id);
CREATE INDEX IF NOT EXISTS ix_ventas_destinatario_id      ON core.ventas (destinatario_id);
CREATE INDEX IF NOT EXISTS ix_ventas_tipo_id              ON core.ventas (tipo_id);
CREATE INDEX IF NOT EXISTS ix_ventas_estado_venta_id      ON core.ventas (estado_venta_id);
CREATE INDEX IF NOT EXISTS ix_ventas_estado_despacho_id   ON core.ventas (estado_despacho_id);
CREATE INDEX IF NOT EXISTS ix_ventas_estado_facturacion_id ON core.ventas (estado_facturacion_id);
CREATE INDEX IF NOT EXISTS ix_ventas_estado_pago_id       ON core.ventas (estado_pago_id);
CREATE INDEX IF NOT EXISTS ix_ventas_ingest_event_id      ON core.ventas (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_ventas_items_venta_id       ON core.ventas_items (venta_id);
CREATE INDEX IF NOT EXISTS ix_ventas_items_producto_id    ON core.ventas_items (producto_id);
CREATE INDEX IF NOT EXISTS ix_ventas_items_variedad_id    ON core.ventas_items (variedad_id);
CREATE INDEX IF NOT EXISTS ix_ventas_items_categoria_id   ON core.ventas_items (categoria_id);
CREATE INDEX IF NOT EXISTS ix_ventas_items_calibre_id     ON core.ventas_items (calibre_id);
CREATE INDEX IF NOT EXISTS ix_ventas_items_codigo_id      ON core.ventas_items (codigo_id);
CREATE INDEX IF NOT EXISTS ix_ventas_items_ingest_event_id ON core.ventas_items (ingest_event_id);

-- --- PAGOS ---
CREATE INDEX IF NOT EXISTS ix_pagos_fecha            ON core.pagos (fecha);
CREATE INDEX IF NOT EXISTS ix_pagos_cliente_id       ON core.pagos (cliente_id);
CREATE INDEX IF NOT EXISTS ix_pagos_medio_pago_id    ON core.pagos (medio_pago_id);
CREATE INDEX IF NOT EXISTS ix_pagos_ingest_event_id  ON core.pagos (ingest_event_id);

-- aplicaciones_pago: el UNIQUE (pago_id, venta_id) no ayuda a filtrar SOLO por venta_id
CREATE INDEX IF NOT EXISTS ix_aplicaciones_pago_venta_id  ON core.aplicaciones_pago (venta_id);
CREATE INDEX IF NOT EXISTS ix_aplicaciones_pago_pago_id   ON core.aplicaciones_pago (pago_id);
CREATE INDEX IF NOT EXISTS ix_aplicaciones_pago_ingest_event_id ON core.aplicaciones_pago (ingest_event_id);

-- --- COMPRAS / INGRESOS / MERMAS ---
CREATE INDEX IF NOT EXISTS ix_compras_producto_fecha        ON core.compras_producto (fecha);
CREATE INDEX IF NOT EXISTS ix_compras_producto_codigo_id    ON core.compras_producto (codigo_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_proveedor_id ON core.compras_producto (proveedor_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_estado_compra_id ON core.compras_producto (estado_compra_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_ingest_event_id ON core.compras_producto (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_compras_producto_items_compra_id   ON core.compras_producto_items (compra_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_items_producto_id ON core.compras_producto_items (producto_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_items_variedad_id ON core.compras_producto_items (variedad_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_items_categoria_id ON core.compras_producto_items (categoria_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_items_calibre_id  ON core.compras_producto_items (calibre_id);
CREATE INDEX IF NOT EXISTS ix_compras_producto_items_ingest_event_id ON core.compras_producto_items (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_ingresos_producto_fecha       ON core.ingresos_producto (fecha);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_ingest_event_id ON core.ingresos_producto (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_ingreso_id ON core.ingresos_producto_items (ingreso_id);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_codigo_id  ON core.ingresos_producto_items (codigo_id);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_producto_id ON core.ingresos_producto_items (producto_id);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_variedad_id ON core.ingresos_producto_items (variedad_id);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_categoria_id ON core.ingresos_producto_items (categoria_id);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_calibre_id  ON core.ingresos_producto_items (calibre_id);
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_items_ingest_event_id ON core.ingresos_producto_items (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_mermas_fecha            ON core.mermas (fecha);
CREATE INDEX IF NOT EXISTS ix_mermas_codigo_id        ON core.mermas (codigo_id);
CREATE INDEX IF NOT EXISTS ix_mermas_producto_id      ON core.mermas (producto_id);
CREATE INDEX IF NOT EXISTS ix_mermas_ingest_event_id  ON core.mermas (ingest_event_id);

-- --- EGRESOS ---
CREATE INDEX IF NOT EXISTS ix_egresos_fecha           ON core.egresos (fecha);
CREATE INDEX IF NOT EXISTS ix_egresos_tipo_egreso_id  ON core.egresos (tipo_egreso_id);
CREATE INDEX IF NOT EXISTS ix_egresos_tipo_economico_id ON core.egresos (tipo_economico_id);
CREATE INDEX IF NOT EXISTS ix_egresos_medio_pago_id   ON core.egresos (medio_pago_id);
CREATE INDEX IF NOT EXISTS ix_egresos_vehiculo_id     ON core.egresos (vehiculo_id);
CREATE INDEX IF NOT EXISTS ix_egresos_trabajador_id   ON core.egresos (trabajador_id);
CREATE INDEX IF NOT EXISTS ix_egresos_vendedor_id     ON core.egresos (vendedor_id);
CREATE INDEX IF NOT EXISTS ix_egresos_cliente_id      ON core.egresos (cliente_id);
CREATE INDEX IF NOT EXISTS ix_egresos_proveedor_id    ON core.egresos (proveedor_id);
CREATE INDEX IF NOT EXISTS ix_egresos_ingest_event_id ON core.egresos (ingest_event_id);

CREATE INDEX IF NOT EXISTS ix_pagos_compra_compra_id  ON core.pagos_compra (compra_id);
CREATE INDEX IF NOT EXISTS ix_pagos_compra_egreso_id  ON core.pagos_compra (egreso_id);
CREATE INDEX IF NOT EXISTS ix_pagos_compra_ingest_event_id ON core.pagos_compra (ingest_event_id);

-- --- EVENTOS / AUDITORÍA ---
CREATE INDEX IF NOT EXISTS ix_entity_events_ingest_event_id ON ingest.entity_events (ingest_event_id);
CREATE INDEX IF NOT EXISTS ix_entity_events_event_at        ON ingest.entity_events (event_at DESC);

CREATE INDEX IF NOT EXISTS ix_audit_log_occurred_at          ON audit.audit_log (occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_audit_log_table_occurred_at    ON audit.audit_log (table_name, occurred_at DESC);

-- --- SOFT DELETE (is_deleted) ---
CREATE INDEX IF NOT EXISTS ix_pedidos_fecha_alive ON core.pedidos (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_entregas_fecha_alive ON core.entregas (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_ventas_fecha_alive ON core.ventas (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_pagos_fecha_alive ON core.pagos (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_compras_producto_fecha_alive ON core.compras_producto (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_ingresos_producto_fecha_alive ON core.ingresos_producto (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_mermas_fecha_alive ON core.mermas (fecha) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_egresos_fecha_alive ON core.egresos (fecha) WHERE deleted_at IS NULL;
