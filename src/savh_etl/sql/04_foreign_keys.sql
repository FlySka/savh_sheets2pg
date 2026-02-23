-- 04_foreign_keys.sql
-- -----------------------------------------------------------------------------
-- Foreign keys (integridad referencial).
--
-- Nota:
--   - Este archivo asume que corres sobre un schema recién creado (drop+create),
--     o que no existen aún las FK. Si lo re-ejecutas sin resetear, fallará.
-- -----------------------------------------------------------------------------

ALTER TABLE core.pedidos ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.pedidos_items ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.entregas ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.ventas ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.ventas_items ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.pagos ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.aplicaciones_pago ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.compras_producto ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.compras_producto_items ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.ingresos_producto ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.ingresos_producto_items ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.mermas ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.egresos ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.pagos_compra ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE ingest.entity_events ADD FOREIGN KEY (actor_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE audit.audit_log ADD FOREIGN KEY (actor_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.pedidos ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.pedidos_items ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.entregas ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.ventas ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.ventas_items ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.pagos ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.aplicaciones_pago ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.compras_producto ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.compras_producto_items ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.ingresos_producto ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.ingresos_producto_items ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.mermas ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.egresos ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.pagos_compra ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE audit.audit_log ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE ingest.entity_events ADD FOREIGN KEY (entity_type_id) REFERENCES core.dim_entity_type (id) ON DELETE RESTRICT;

ALTER TABLE ingest.entity_events ADD FOREIGN KEY (event_type_id) REFERENCES core.dim_event_type (id) ON DELETE RESTRICT;

ALTER TABLE ingest.entity_events ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.dim_variedades ADD FOREIGN KEY (producto_id) REFERENCES core.productos (id) ON DELETE RESTRICT;

ALTER TABLE core.dim_categorias ADD FOREIGN KEY (producto_id) REFERENCES core.productos (id) ON DELETE RESTRICT;

ALTER TABLE core.dim_calibres ADD FOREIGN KEY (producto_id) REFERENCES core.productos (id) ON DELETE RESTRICT;

ALTER TABLE core.dim_calibres ADD FOREIGN KEY (convencion_id) REFERENCES core.dim_convenciones (id) ON DELETE RESTRICT;

ALTER TABLE core.pedidos_items ADD FOREIGN KEY (codigo_id) REFERENCES core.codigos (id) ON DELETE RESTRICT;

ALTER TABLE core.ventas_items ADD FOREIGN KEY (codigo_id) REFERENCES core.codigos (id) ON DELETE RESTRICT;

ALTER TABLE core.compras_producto ADD FOREIGN KEY (codigo_id) REFERENCES core.codigos (id) ON DELETE RESTRICT;

ALTER TABLE core.ingresos_producto_items ADD FOREIGN KEY (codigo_id) REFERENCES core.codigos (id) ON DELETE RESTRICT;

ALTER TABLE core.mermas ADD FOREIGN KEY (codigo_id) REFERENCES core.codigos (id) ON DELETE RESTRICT;

ALTER TABLE core.dim_tipos_egreso ADD FOREIGN KEY (grupo_id) REFERENCES core.dim_grupos_egresos (id) ON DELETE RESTRICT;

ALTER TABLE core.terceros_cliente ADD FOREIGN KEY (tercero_id) REFERENCES core.terceros (id) ON DELETE CASCADE;

ALTER TABLE core.terceros_proveedor ADD FOREIGN KEY (tercero_id) REFERENCES core.terceros (id) ON DELETE CASCADE;

ALTER TABLE core.terceros_vendedor ADD FOREIGN KEY (tercero_id) REFERENCES core.terceros (id) ON DELETE CASCADE;

ALTER TABLE core.terceros_cliente ADD FOREIGN KEY (tipo_id) REFERENCES core.dim_tipos_clientes (id) ON DELETE RESTRICT;

ALTER TABLE core.terceros_cliente ADD FOREIGN KEY (vendedor_id) REFERENCES core.terceros_vendedor (tercero_id) ON DELETE SET NULL;

ALTER TABLE core.terceros_vendedor ADD FOREIGN KEY (modelo_comercial_id) REFERENCES core.dim_modelos_comerciales (id) ON DELETE RESTRICT;

ALTER TABLE core.destinatarios ADD FOREIGN KEY (cliente_id) REFERENCES core.terceros_cliente (tercero_id) ON DELETE RESTRICT;

ALTER TABLE core.pedidos ADD FOREIGN KEY (cliente_id) REFERENCES core.terceros_cliente (tercero_id) ON DELETE RESTRICT;

ALTER TABLE core.pedidos ADD FOREIGN KEY (destinatario_id) REFERENCES core.destinatarios (id) ON DELETE SET NULL;

ALTER TABLE core.pedidos ADD FOREIGN KEY (estado_id) REFERENCES core.dim_estados_pedido (id) ON DELETE RESTRICT;

ALTER TABLE core.pedidos_items ADD FOREIGN KEY (pedido_id) REFERENCES core.pedidos (id) ON DELETE CASCADE;

ALTER TABLE core.pedidos_items ADD FOREIGN KEY (producto_id) REFERENCES core.productos (id) ON DELETE RESTRICT;

ALTER TABLE core.pedidos_items ADD FOREIGN KEY (variedad_id) REFERENCES core.dim_variedades (id) ON DELETE RESTRICT;

ALTER TABLE core.pedidos_items ADD FOREIGN KEY (categoria_id) REFERENCES core.dim_categorias (id) ON DELETE RESTRICT;

ALTER TABLE core.pedidos_items ADD FOREIGN KEY (calibre_id) REFERENCES core.dim_calibres (id) ON DELETE RESTRICT;

ALTER TABLE core.entregas ADD FOREIGN KEY (pedido_id) REFERENCES core.pedidos (id) ON DELETE CASCADE;

ALTER TABLE core.ventas ADD FOREIGN KEY (pedido_id) REFERENCES core.pedidos (id) ON DELETE SET NULL;

ALTER TABLE core.ventas ADD FOREIGN KEY (cliente_id) REFERENCES core.terceros_cliente (tercero_id) ON DELETE RESTRICT;

ALTER TABLE core.ventas ADD FOREIGN KEY (destinatario_id) REFERENCES core.destinatarios (id) ON DELETE SET NULL;

ALTER TABLE core.ventas ADD FOREIGN KEY (tipo_id) REFERENCES core.dim_tipos_venta (id) ON DELETE RESTRICT;

ALTER TABLE core.ventas ADD FOREIGN KEY (estado_venta_id) REFERENCES core.dim_estados_venta (id) ON DELETE RESTRICT;

ALTER TABLE core.ventas ADD FOREIGN KEY (estado_despacho_id) REFERENCES core.dim_estados_despacho (id) ON DELETE RESTRICT;

ALTER TABLE core.ventas ADD FOREIGN KEY (estado_facturacion_id) REFERENCES core.dim_estados_venta_facturacion (id) ON DELETE RESTRICT;

ALTER TABLE core.ventas ADD FOREIGN KEY (estado_pago_id) REFERENCES core.dim_estados_venta_pago (id) ON DELETE RESTRICT;

ALTER TABLE core.ventas_items ADD FOREIGN KEY (venta_id) REFERENCES core.ventas (id) ON DELETE CASCADE;

ALTER TABLE core.ventas_items ADD FOREIGN KEY (producto_id) REFERENCES core.productos (id) ON DELETE RESTRICT;

ALTER TABLE core.ventas_items ADD FOREIGN KEY (variedad_id) REFERENCES core.dim_variedades (id) ON DELETE RESTRICT;

ALTER TABLE core.ventas_items ADD FOREIGN KEY (categoria_id) REFERENCES core.dim_categorias (id) ON DELETE RESTRICT;

ALTER TABLE core.ventas_items ADD FOREIGN KEY (calibre_id) REFERENCES core.dim_calibres (id) ON DELETE RESTRICT;

ALTER TABLE core.pagos ADD FOREIGN KEY (cliente_id) REFERENCES core.terceros_cliente (tercero_id) ON DELETE RESTRICT;

ALTER TABLE core.pagos ADD FOREIGN KEY (medio_pago_id) REFERENCES core.dim_medios_pago (id) ON DELETE RESTRICT;

ALTER TABLE core.aplicaciones_pago ADD FOREIGN KEY (pago_id) REFERENCES core.pagos (id) ON DELETE CASCADE;

ALTER TABLE core.aplicaciones_pago ADD FOREIGN KEY (venta_id) REFERENCES core.ventas (id) ON DELETE CASCADE;

ALTER TABLE core.compras_producto ADD FOREIGN KEY (proveedor_id) REFERENCES core.terceros_proveedor (tercero_id) ON DELETE SET NULL;

ALTER TABLE core.compras_producto ADD FOREIGN KEY (estado_compra_id) REFERENCES core.dim_estados_compra (id) ON DELETE RESTRICT;

ALTER TABLE core.compras_producto_items ADD FOREIGN KEY (compra_id) REFERENCES core.compras_producto (id) ON DELETE CASCADE;

ALTER TABLE core.compras_producto_items ADD FOREIGN KEY (producto_id) REFERENCES core.productos (id) ON DELETE RESTRICT;

ALTER TABLE core.compras_producto_items ADD FOREIGN KEY (variedad_id) REFERENCES core.dim_variedades (id) ON DELETE RESTRICT;

ALTER TABLE core.compras_producto_items ADD FOREIGN KEY (categoria_id) REFERENCES core.dim_categorias (id) ON DELETE RESTRICT;

ALTER TABLE core.compras_producto_items ADD FOREIGN KEY (calibre_id) REFERENCES core.dim_calibres (id) ON DELETE RESTRICT;

ALTER TABLE core.ingresos_producto_items ADD FOREIGN KEY (ingreso_id) REFERENCES core.ingresos_producto (id) ON DELETE CASCADE;

ALTER TABLE core.ingresos_producto_items ADD FOREIGN KEY (producto_id) REFERENCES core.productos (id) ON DELETE RESTRICT;

ALTER TABLE core.ingresos_producto_items ADD FOREIGN KEY (variedad_id) REFERENCES core.dim_variedades (id) ON DELETE RESTRICT;

ALTER TABLE core.ingresos_producto_items ADD FOREIGN KEY (categoria_id) REFERENCES core.dim_categorias (id) ON DELETE RESTRICT;

ALTER TABLE core.ingresos_producto_items ADD FOREIGN KEY (calibre_id) REFERENCES core.dim_calibres (id) ON DELETE RESTRICT;

ALTER TABLE core.mermas ADD FOREIGN KEY (producto_id) REFERENCES core.productos (id) ON DELETE RESTRICT;

ALTER TABLE core.mermas ADD FOREIGN KEY (variedad_id) REFERENCES core.dim_variedades (id) ON DELETE RESTRICT;

ALTER TABLE core.mermas ADD FOREIGN KEY (categoria_id) REFERENCES core.dim_categorias (id) ON DELETE RESTRICT;

ALTER TABLE core.mermas ADD FOREIGN KEY (calibre_id) REFERENCES core.dim_calibres (id) ON DELETE RESTRICT;

ALTER TABLE core.egresos ADD FOREIGN KEY (tipo_egreso_id) REFERENCES core.dim_tipos_egreso (id) ON DELETE RESTRICT;

ALTER TABLE core.egresos ADD FOREIGN KEY (tipo_economico_id) REFERENCES core.dim_tipos_economicos (id) ON DELETE RESTRICT;

ALTER TABLE core.egresos ADD FOREIGN KEY (medio_pago_id) REFERENCES core.dim_medios_pago (id) ON DELETE RESTRICT;

ALTER TABLE core.egresos ADD FOREIGN KEY (vehiculo_id) REFERENCES core.vehiculos (id) ON DELETE SET NULL;

ALTER TABLE core.egresos ADD FOREIGN KEY (trabajador_id) REFERENCES core.trabajadores (id) ON DELETE SET NULL;

ALTER TABLE core.egresos ADD FOREIGN KEY (vendedor_id) REFERENCES core.terceros_vendedor (tercero_id) ON DELETE SET NULL;

ALTER TABLE core.egresos ADD FOREIGN KEY (cliente_id) REFERENCES core.terceros_cliente (tercero_id) ON DELETE SET NULL;

ALTER TABLE core.egresos ADD FOREIGN KEY (proveedor_id) REFERENCES core.terceros_proveedor (tercero_id) ON DELETE SET NULL;

ALTER TABLE core.pagos_compra ADD FOREIGN KEY (compra_id) REFERENCES core.compras_producto (id) ON DELETE CASCADE;

ALTER TABLE core.pagos_compra ADD FOREIGN KEY (egreso_id) REFERENCES core.egresos (id) ON DELETE CASCADE;
