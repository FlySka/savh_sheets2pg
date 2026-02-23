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
  ALTER TABLE core.terceros_vendedor
    ADD CONSTRAINT chk_terceros_vendedor_comision
    CHECK (comision >= 0 AND comision <= 1);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- =====================
-- 🧾 Pedidos
-- =====================
DO $$
BEGIN
  ALTER TABLE core.pedidos_items
    ADD CONSTRAINT chk_pedidos_items_kg
    CHECK (kg >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.pedidos_items
    ADD CONSTRAINT chk_pedidos_items_precio_unit
    CHECK (precio_unit >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.pedidos_items
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
  ALTER TABLE core.ventas_items
    ADD CONSTRAINT chk_ventas_items_kg
    CHECK (kg >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.ventas_items
    ADD CONSTRAINT chk_ventas_items_precio_unit
    CHECK (precio_unit >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.ventas_items
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
  ALTER TABLE core.pagos
    ADD CONSTRAINT chk_pagos_monto
    CHECK (monto >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.aplicaciones_pago
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
  ALTER TABLE core.compras_producto_items
    ADD CONSTRAINT chk_compras_items_kg
    CHECK (kg >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.compras_producto_items
    ADD CONSTRAINT chk_compras_items_costo_unit
    CHECK (costo_unit >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.compras_producto_items
    ADD CONSTRAINT chk_compras_items_costo_unit_con_iva
    CHECK (costo_unit_con_iva >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.compras_producto_items
    ADD CONSTRAINT chk_compras_items_costo_total
    CHECK (costo_total >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.compras_producto_items
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
  ALTER TABLE core.ingresos_producto_items
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
  ALTER TABLE core.mermas
    ADD CONSTRAINT chk_mermas_kg
    CHECK (kg >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.mermas
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
  ALTER TABLE core.egresos
    ADD CONSTRAINT chk_egresos_monto
    CHECK (monto >= 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE core.pagos_compra
    ADD CONSTRAINT chk_pagos_compra_monto_aplicado
    CHECK (monto_aplicado > 0);
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;
