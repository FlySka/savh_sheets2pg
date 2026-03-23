COMMENT ON COLUMN core.salesperson_contracts.tasa_comision IS 'CHECK (tasa_comision >= 0)';

COMMENT ON COLUMN core.order_items.kg IS 'CHECK (kg >= 0)';

COMMENT ON COLUMN core.order_items.precio_unit IS 'CHECK (precio_unit >= 0)';

COMMENT ON COLUMN core.order_items.precio_total IS 'CHECK (precio_total >= 0)';

COMMENT ON COLUMN core.sale_items.kg IS 'CHECK (kg >= 0)';

COMMENT ON COLUMN core.sale_items.precio_unit IS 'CHECK (precio_unit >= 0)';

COMMENT ON COLUMN core.sale_items.precio_total IS 'CHECK (precio_total >= 0)';

COMMENT ON COLUMN core.payments.monto IS 'CHECK (monto >= 0)';

COMMENT ON COLUMN core.product_purchase_items.kg IS 'CHECK (kg >= 0)';

COMMENT ON COLUMN core.product_purchase_items.costo_unit IS 'CHECK (costo_unit >= 0)';

COMMENT ON COLUMN core.product_purchase_items.costo_unit_con_iva IS 'CHECK (costo_unit_con_iva >= 0)';

COMMENT ON COLUMN core.product_purchase_items.costo_total IS 'CHECK (costo_total >= 0)';

COMMENT ON COLUMN core.product_purchase_items.costo_total_con_iva IS 'CHECK (costo_total_con_iva >= 0)';

COMMENT ON COLUMN core.product_receipt_items.kg IS 'CHECK (kg >= 0)';

COMMENT ON COLUMN core.product_losses.kg IS 'CHECK (kg >= 0)';

COMMENT ON COLUMN core.product_losses.kg_ajustado IS 'CHECK (kg_ajustado >= 0)';

COMMENT ON COLUMN core.expenses.monto IS 'CHECK (monto >= 0)';

COMMENT ON COLUMN core.purchase_payment_applications.monto_aplicado IS 'CHECK (monto_aplicado > 0)';
