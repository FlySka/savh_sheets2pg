-- 01_schema.sql
-- -----------------------------------------------------------------------------
-- Bootstrapping de esquemas base para SAVH
--   - core  : tablas del dominio/negocio
--   - ingest: trazabilidad / idempotencia de ingestas
--   - audit : auditoría forense (logs de cambios)
--
-- Nota importante:
--   1) Este archivo es *solo SQL* (sin comandos \set / \ir de psql), para que
--      puedas ejecutarlo desde pgAdmin o desde tu ETL (SQLAlchemy/psycopg).
--   2) "SET search_path" es por *sesión*. Si ejecutas archivos en conexiones
--      distintas, repite el SET en cada archivo (o califica tablas con schema.).
-- -----------------------------------------------------------------------------

-- 1) Schemas (idempotente)
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS audit;


-- 2) Time zone: mejor persistente (no solo para esta sesión)
-- OJO: esto requiere privilegios sobre la DB.
DO $$
BEGIN
  EXECUTE format('ALTER DATABASE %I SET timezone TO %L', current_database(), 'UTC');
EXCEPTION WHEN insufficient_privilege THEN
  RAISE NOTICE 'No se pudo fijar timezone=UTC a nivel DB (insufficient_privilege).';
END $$;

-- (Opcional) Si quieres que tus scripts asuman search_path consistente al correrlos a mano:
-- Esto también es sesión; útil para psql/pgAdmin, no para SQLAlchemy pool.
SET search_path TO core, public;

-- 3) Extensiones opcionales (best-effort)
DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS pgcrypto;
EXCEPTION WHEN insufficient_privilege THEN
  RAISE NOTICE 'No se pudo crear extensión pgcrypto (insufficient_privilege). Continúo sin ella.';
END $$;

DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS citext;
EXCEPTION WHEN insufficient_privilege THEN
  RAISE NOTICE 'No se pudo crear extensión citext (insufficient_privilege). Continúo sin ella.';
END $$;