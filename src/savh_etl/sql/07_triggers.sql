-- 07_triggers.sql
-- -----------------------------------------------------------------------------
-- Triggers recomendados:
--   1) updated_at automático en UPDATE
--
-- Nota:
--   - Crea triggers en tablas de core/ingest/audit que tengan columna updated_at.
--   - Script idempotente: puedes correrlo muchas veces.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.fn_set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$;

DO $$
DECLARE
  r record;
  trg_name text;
BEGIN
  FOR r IN
    SELECT c.table_schema, c.table_name
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema
     AND t.table_name   = c.table_name
    WHERE c.table_schema IN ('core','ingest','audit')
      AND c.column_name  = 'updated_at'
      AND t.table_type   = 'BASE TABLE'
  LOOP
    trg_name := format('trg_%s_set_updated_at', r.table_name);

    EXECUTE format(
      'DROP TRIGGER IF EXISTS %I ON %I.%I',
      trg_name, r.table_schema, r.table_name
    );

    EXECUTE format(
      'CREATE TRIGGER %I
         BEFORE UPDATE ON %I.%I
         FOR EACH ROW
         EXECUTE FUNCTION core.fn_set_updated_at()',
      trg_name, r.table_schema, r.table_name
    );
  END LOOP;
END $$;

