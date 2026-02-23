COMMENT ON COLUMN core.terceros_vendedor.comision IS 'CHECK (comision >= 0 AND comision <= 1)';

COMMENT ON COLUMN core.pedidos_items.kg IS 'CHECK (kg >= 0)';

COMMENT ON COLUMN core.pedidos_items.precio_unit IS 'CHECK (precio_unit >= 0)';

COMMENT ON COLUMN core.pedidos_items.precio_total IS 'CHECK (precio_total >= 0)';

COMMENT ON COLUMN core.ventas_items.kg IS 'CHECK (kg >= 0)';

COMMENT ON COLUMN core.ventas_items.precio_unit IS 'CHECK (precio_unit >= 0)';

COMMENT ON COLUMN core.ventas_items.precio_total IS 'CHECK (precio_total >= 0)';

COMMENT ON COLUMN core.pagos.monto IS 'CHECK (monto >= 0)';

COMMENT ON COLUMN core.aplicaciones_pago.monto_aplicado IS 'CHECK (monto_aplicado > 0)';

COMMENT ON COLUMN core.compras_producto_items.kg IS 'CHECK (kg >= 0)';

COMMENT ON COLUMN core.compras_producto_items.costo_unit IS 'CHECK (costo_unit >= 0)';

COMMENT ON COLUMN core.compras_producto_items.costo_unit_con_iva IS 'CHECK (costo_unit_con_iva >= 0)';

COMMENT ON COLUMN core.compras_producto_items.costo_total IS 'CHECK (costo_total >= 0)';

COMMENT ON COLUMN core.compras_producto_items.costo_total_con_iva IS 'CHECK (costo_total_con_iva >= 0)';

COMMENT ON COLUMN core.ingresos_producto_items.kg IS 'CHECK (kg >= 0)';

COMMENT ON COLUMN core.mermas.kg IS 'CHECK (kg >= 0)';

COMMENT ON COLUMN core.mermas.kg_ajustado IS 'CHECK (kg_ajustado >= 0)';

COMMENT ON COLUMN core.egresos.monto IS 'CHECK (monto >= 0)';

COMMENT ON COLUMN core.pagos_compra.monto_aplicado IS 'CHECK (monto_aplicado > 0)';
