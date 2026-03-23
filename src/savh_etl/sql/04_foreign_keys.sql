-- 04_foreign_keys.sql
-- -----------------------------------------------------------------------------
-- Foreign keys (integridad referencial).
--
-- Nota:
--   - Este archivo asume que corres sobre un schema recién creado (drop+create),
--     o que no existen aún las FK. Si lo re-ejecutas sin resetear, fallará.
-- -----------------------------------------------------------------------------

ALTER TABLE core.orders ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.order_items ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.deliveries ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.sales ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.sale_items ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.payments ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.product_purchases ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.product_purchase_items ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.product_receipts ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.product_receipt_items ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.product_losses ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.expenses ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.purchase_payment_applications ADD FOREIGN KEY (deleted_by_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE ingest.entity_events ADD FOREIGN KEY (actor_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE audit.audit_log ADD FOREIGN KEY (actor_user_id) REFERENCES core.app_users (id) ON DELETE SET NULL;

ALTER TABLE core.orders ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.order_items ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.deliveries ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.sales ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.sale_items ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.payments ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.product_purchases ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.product_purchase_items ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.product_receipts ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.product_receipt_items ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.product_losses ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.expenses ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.purchase_payment_applications ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE audit.audit_log ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE ingest.entity_events ADD FOREIGN KEY (entity_type_id) REFERENCES core.dim_entity_type (id) ON DELETE RESTRICT;

ALTER TABLE ingest.entity_events ADD FOREIGN KEY (event_type_id) REFERENCES core.dim_event_type (id) ON DELETE RESTRICT;

ALTER TABLE ingest.entity_events ADD FOREIGN KEY (ingest_event_id) REFERENCES ingest.ingest_events (id) ON DELETE SET NULL;

ALTER TABLE core.dim_varieties ADD FOREIGN KEY (producto_id) REFERENCES core.products (id) ON DELETE RESTRICT;

ALTER TABLE core.dim_categories ADD FOREIGN KEY (producto_id) REFERENCES core.products (id) ON DELETE RESTRICT;

ALTER TABLE core.dim_calibers ADD FOREIGN KEY (producto_id) REFERENCES core.products (id) ON DELETE RESTRICT;

ALTER TABLE core.dim_calibers ADD FOREIGN KEY (convencion_id) REFERENCES core.dim_conventions (id) ON DELETE RESTRICT;

ALTER TABLE core.order_items ADD FOREIGN KEY (codigo_id) REFERENCES core.codes (id) ON DELETE RESTRICT;

ALTER TABLE core.sale_items ADD FOREIGN KEY (codigo_id) REFERENCES core.codes (id) ON DELETE RESTRICT;

ALTER TABLE core.product_purchases ADD FOREIGN KEY (codigo_id) REFERENCES core.codes (id) ON DELETE RESTRICT;

ALTER TABLE core.product_receipt_items ADD FOREIGN KEY (codigo_id) REFERENCES core.codes (id) ON DELETE RESTRICT;

ALTER TABLE core.product_losses ADD FOREIGN KEY (codigo_id) REFERENCES core.codes (id) ON DELETE RESTRICT;

ALTER TABLE core.dim_expense_types ADD FOREIGN KEY (grupo_id) REFERENCES core.dim_expense_groups (id) ON DELETE RESTRICT;

ALTER TABLE core.assets ADD FOREIGN KEY (tipo_activo_id) REFERENCES core.dim_type_assets (id) ON DELETE RESTRICT;

ALTER TABLE core.assets_vehicles ADD FOREIGN KEY (activo_id) REFERENCES core.assets (id) ON DELETE CASCADE;

ALTER TABLE core.parties_customer ADD FOREIGN KEY (tercero_id) REFERENCES core.parties (id) ON DELETE CASCADE;

ALTER TABLE core.parties_supplier ADD FOREIGN KEY (tercero_id) REFERENCES core.parties (id) ON DELETE CASCADE;

ALTER TABLE core.parties_salesperson ADD FOREIGN KEY (tercero_id) REFERENCES core.parties (id) ON DELETE CASCADE;

ALTER TABLE core.parties_employee ADD FOREIGN KEY (tercero_id) REFERENCES core.parties (id) ON DELETE CASCADE;

ALTER TABLE core.parties_customer ADD FOREIGN KEY (tipo_id) REFERENCES core.dim_customer_types (id) ON DELETE RESTRICT;

ALTER TABLE core.parties_customer ADD FOREIGN KEY (vendedor_id) REFERENCES core.parties_salesperson (tercero_id) ON DELETE SET NULL;

ALTER TABLE core.parties_salesperson ADD FOREIGN KEY (modelo_comercial_id) REFERENCES core.dim_commercial_models (id) ON DELETE RESTRICT;

ALTER TABLE core.salesperson_contracts ADD FOREIGN KEY (vendedor_id) REFERENCES core.parties_salesperson (tercero_id) ON DELETE CASCADE;

ALTER TABLE core.salesperson_contracts ADD FOREIGN KEY (tipo_id) REFERENCES core.dim_salesperson_contract_types (id) ON DELETE RESTRICT;

ALTER TABLE core.salesperson_contracts ADD FOREIGN KEY (base_id) REFERENCES core.dim_salesperson_commission_bases (id) ON DELETE RESTRICT;

ALTER TABLE core.parties_recipient ADD FOREIGN KEY (tercero_id) REFERENCES core.parties (id) ON DELETE CASCADE;

ALTER TABLE core.parties_recipient ADD FOREIGN KEY (cliente_id) REFERENCES core.parties_customer (tercero_id) ON DELETE RESTRICT;

ALTER TABLE core.orders ADD FOREIGN KEY (cliente_id) REFERENCES core.parties_customer (tercero_id) ON DELETE RESTRICT;

ALTER TABLE core.orders ADD FOREIGN KEY (destinatario_id) REFERENCES core.parties_recipient (tercero_id) ON DELETE SET NULL;

ALTER TABLE core.orders ADD FOREIGN KEY (estado_id) REFERENCES core.dim_order_statuses (id) ON DELETE RESTRICT;

ALTER TABLE core.order_items ADD FOREIGN KEY (pedido_id) REFERENCES core.orders (id) ON DELETE CASCADE;

ALTER TABLE core.order_items ADD FOREIGN KEY (producto_id) REFERENCES core.products (id) ON DELETE RESTRICT;

ALTER TABLE core.order_items ADD FOREIGN KEY (variedad_id) REFERENCES core.dim_varieties (id) ON DELETE RESTRICT;

ALTER TABLE core.order_items ADD FOREIGN KEY (categoria_id) REFERENCES core.dim_categories (id) ON DELETE RESTRICT;

ALTER TABLE core.order_items ADD FOREIGN KEY (calibre_id) REFERENCES core.dim_calibers (id) ON DELETE RESTRICT;

ALTER TABLE core.deliveries ADD FOREIGN KEY (pedido_id) REFERENCES core.orders (id) ON DELETE CASCADE;

ALTER TABLE core.sales ADD FOREIGN KEY (pedido_id) REFERENCES core.orders (id) ON DELETE SET NULL;

ALTER TABLE core.sales ADD FOREIGN KEY (cliente_id) REFERENCES core.parties_customer (tercero_id) ON DELETE RESTRICT;

ALTER TABLE core.sales ADD FOREIGN KEY (destinatario_id) REFERENCES core.parties_recipient (tercero_id) ON DELETE SET NULL;

ALTER TABLE core.sales ADD FOREIGN KEY (tipo_id) REFERENCES core.dim_sale_types (id) ON DELETE RESTRICT;

ALTER TABLE core.sales ADD FOREIGN KEY (estado_venta_id) REFERENCES core.dim_sales_statuses (id) ON DELETE RESTRICT;

ALTER TABLE core.sales ADD FOREIGN KEY (estado_despacho_id) REFERENCES core.dim_shipping_statuses (id) ON DELETE RESTRICT;

ALTER TABLE core.sales ADD FOREIGN KEY (estado_facturacion_id) REFERENCES core.dim_sales_billing_statuses (id) ON DELETE RESTRICT;

ALTER TABLE core.sales ADD FOREIGN KEY (estado_pago_id) REFERENCES core.dim_sales_payment_statuses (id) ON DELETE RESTRICT;

ALTER TABLE core.sale_items ADD FOREIGN KEY (venta_id) REFERENCES core.sales (id) ON DELETE CASCADE;

ALTER TABLE core.sale_items ADD FOREIGN KEY (producto_id) REFERENCES core.products (id) ON DELETE RESTRICT;

ALTER TABLE core.sale_items ADD FOREIGN KEY (variedad_id) REFERENCES core.dim_varieties (id) ON DELETE RESTRICT;

ALTER TABLE core.sale_items ADD FOREIGN KEY (categoria_id) REFERENCES core.dim_categories (id) ON DELETE RESTRICT;

ALTER TABLE core.sale_items ADD FOREIGN KEY (calibre_id) REFERENCES core.dim_calibers (id) ON DELETE RESTRICT;

ALTER TABLE core.payments ADD FOREIGN KEY (cliente_id) REFERENCES core.parties_customer (tercero_id) ON DELETE RESTRICT;

ALTER TABLE core.payments ADD FOREIGN KEY (medio_pago_id) REFERENCES core.dim_payment_methods (id) ON DELETE RESTRICT;

ALTER TABLE core.product_purchases ADD FOREIGN KEY (proveedor_id) REFERENCES core.parties_supplier (tercero_id) ON DELETE SET NULL;

ALTER TABLE core.product_purchases ADD FOREIGN KEY (estado_compra_id) REFERENCES core.dim_purchase_statuses (id) ON DELETE RESTRICT;

ALTER TABLE core.product_purchase_items ADD FOREIGN KEY (compra_id) REFERENCES core.product_purchases (id) ON DELETE CASCADE;

ALTER TABLE core.product_purchase_items ADD FOREIGN KEY (producto_id) REFERENCES core.products (id) ON DELETE RESTRICT;

ALTER TABLE core.product_purchase_items ADD FOREIGN KEY (variedad_id) REFERENCES core.dim_varieties (id) ON DELETE RESTRICT;

ALTER TABLE core.product_purchase_items ADD FOREIGN KEY (categoria_id) REFERENCES core.dim_categories (id) ON DELETE RESTRICT;

ALTER TABLE core.product_purchase_items ADD FOREIGN KEY (calibre_id) REFERENCES core.dim_calibers (id) ON DELETE RESTRICT;

ALTER TABLE core.product_receipt_items ADD FOREIGN KEY (ingreso_id) REFERENCES core.product_receipts (id) ON DELETE CASCADE;

ALTER TABLE core.product_receipt_items ADD FOREIGN KEY (producto_id) REFERENCES core.products (id) ON DELETE RESTRICT;

ALTER TABLE core.product_receipt_items ADD FOREIGN KEY (variedad_id) REFERENCES core.dim_varieties (id) ON DELETE RESTRICT;

ALTER TABLE core.product_receipt_items ADD FOREIGN KEY (categoria_id) REFERENCES core.dim_categories (id) ON DELETE RESTRICT;

ALTER TABLE core.product_receipt_items ADD FOREIGN KEY (calibre_id) REFERENCES core.dim_calibers (id) ON DELETE RESTRICT;

ALTER TABLE core.product_losses ADD FOREIGN KEY (producto_id) REFERENCES core.products (id) ON DELETE RESTRICT;

ALTER TABLE core.product_losses ADD FOREIGN KEY (variedad_id) REFERENCES core.dim_varieties (id) ON DELETE RESTRICT;

ALTER TABLE core.product_losses ADD FOREIGN KEY (categoria_id) REFERENCES core.dim_categories (id) ON DELETE RESTRICT;

ALTER TABLE core.product_losses ADD FOREIGN KEY (calibre_id) REFERENCES core.dim_calibers (id) ON DELETE RESTRICT;

ALTER TABLE core.expenses ADD FOREIGN KEY (tipo_egreso_id) REFERENCES core.dim_expense_types (id) ON DELETE RESTRICT;

ALTER TABLE core.expenses ADD FOREIGN KEY (tipo_economico_id) REFERENCES core.dim_economic_types (id) ON DELETE RESTRICT;

ALTER TABLE core.expenses ADD FOREIGN KEY (medio_pago_id) REFERENCES core.dim_payment_methods (id) ON DELETE RESTRICT;

ALTER TABLE core.expenses ADD FOREIGN KEY (activo_id) REFERENCES core.assets (id) ON DELETE SET NULL;

ALTER TABLE core.expenses ADD FOREIGN KEY (actor_id) REFERENCES core.parties (id) ON DELETE SET NULL;

ALTER TABLE core.purchase_payment_applications ADD FOREIGN KEY (compra_id) REFERENCES core.product_purchases (id) ON DELETE CASCADE;

ALTER TABLE core.purchase_payment_applications ADD FOREIGN KEY (egreso_id) REFERENCES core.expenses (id) ON DELETE CASCADE;
