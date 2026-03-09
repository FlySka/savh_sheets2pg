-- 03_constraints.sql
-- CHECK constraints y reglas de integridad que no están expresadas como NOT NULL / UNIQUE.
--
-- Nota:
--   - Se usan DO $$ ... EXCEPTION WHEN duplicate_object THEN ... para que el script sea re-ejecutable.

-- =====================
-- 🧾 Auditoría
-- =====================
DO $$
BEGIN
  ALTER TABLE audit.audit_log
    ADD CONSTRAINT chk_audit_log_action
    CHECK (action IN ('I','U','D'));
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- =====================
-- 👥 Terceros
-- =====================
DO $$
BEGIN
  ALTER TABLE core.salesperson_contracts
    ADD CONSTRAINT chk_salesperson_contracts_tasa_comision
    CHECK (tasa_comision >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- =====================
-- 🧾 Pedidos
-- =====================
DO $$
BEGIN
  ALTER TABLE core.order_items
    ADD CONSTRAINT chk_pedidos_items_kg
    CHECK (kg >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.order_items
    ADD CONSTRAINT chk_pedidos_items_precio_unit
    CHECK (precio_unit >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.order_items
    ADD CONSTRAINT chk_pedidos_items_precio_total
    CHECK (precio_total >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- =====================
-- 🧾 Ventas
-- =====================
DO $$
BEGIN
  ALTER TABLE core.sale_items
    ADD CONSTRAINT chk_ventas_items_kg
    CHECK (kg >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.sale_items
    ADD CONSTRAINT chk_ventas_items_precio_unit
    CHECK (precio_unit >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.sale_items
    ADD CONSTRAINT chk_ventas_items_precio_total
    CHECK (precio_total >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- =====================
-- 💰 Pagos
-- =====================
DO $$
BEGIN
  ALTER TABLE core.payments
    ADD CONSTRAINT chk_pagos_monto
    CHECK (monto >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.payment_applications
    ADD CONSTRAINT chk_aplicaciones_pago_monto_aplicado
    CHECK (monto_aplicado > 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- =====================
-- 🧾 Compras
-- =====================
DO $$
BEGIN
  ALTER TABLE core.product_purchase_items
    ADD CONSTRAINT chk_compras_items_kg
    CHECK (kg >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.product_purchase_items
    ADD CONSTRAINT chk_compras_items_costo_unit
    CHECK (costo_unit >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.product_purchase_items
    ADD CONSTRAINT chk_compras_items_costo_unit_con_iva
    CHECK (costo_unit_con_iva >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.product_purchase_items
    ADD CONSTRAINT chk_compras_items_costo_total
    CHECK (costo_total >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.product_purchase_items
    ADD CONSTRAINT chk_compras_items_costo_total_con_iva
    CHECK (costo_total_con_iva >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- =====================
-- 📦 Ingresos
-- =====================
DO $$
BEGIN
  ALTER TABLE core.product_receipt_items
    ADD CONSTRAINT chk_ingresos_items_kg
    CHECK (kg >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- =====================
-- 🗑️ Mermas
-- =====================
DO $$
BEGIN
  ALTER TABLE core.product_losses
    ADD CONSTRAINT chk_mermas_kg
    CHECK (kg >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.product_losses
    ADD CONSTRAINT chk_mermas_kg_ajustado
    CHECK (kg_ajustado >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- =====================
-- 💸 Egresos
-- =====================
DO $$
BEGIN
  ALTER TABLE core.expenses
    ADD CONSTRAINT chk_egresos_monto
    CHECK (monto >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.purchase_payment_applications
    ADD CONSTRAINT chk_pagos_compra_monto_aplicado
    CHECK (monto_aplicado > 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;
