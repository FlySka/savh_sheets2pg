#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
PROJECT_ROOT="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${1:-$PROJECT_ROOT/.env}"

if [ ! -f "$ENV_FILE" ]; then
  echo "No se encontró el archivo de entorno: $ENV_FILE" >&2
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "No se encontró 'psql'. Instala el cliente de PostgreSQL." >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "No se encontró 'python3'. Es necesario para parsear el DSN." >&2
  exit 1
fi

dsn_line="$(grep -E '^[[:space:]]*SAVH_ETL_PG_DSN=' "$ENV_FILE" | tail -n 1 || true)"
if [ -z "$dsn_line" ]; then
  echo "No se encontró SAVH_ETL_PG_DSN en $ENV_FILE" >&2
  exit 1
fi

dsn_value="${dsn_line#*=}"
dsn_value="${dsn_value%%#*}"
dsn_value="$(printf '%s' "$dsn_value" | sed -E 's/^[[:space:]]+|[[:space:]]+$//g')"
dsn_value="${dsn_value%\"}"
dsn_value="${dsn_value#\"}"

if [ -z "$dsn_value" ]; then
  echo "SAVH_ETL_PG_DSN está vacío en $ENV_FILE" >&2
  exit 1
fi

parse_result="$(
  python3 - "$dsn_value" <<'PY'
import sys
from urllib.parse import urlsplit, urlunsplit

dsn = sys.argv[1].strip()
if "://" not in dsn:
    raise SystemExit("DSN inválido: falta esquema.")

scheme, rest = dsn.split("://", 1)
if scheme.startswith("postgresql+"):
    dsn = f"postgresql://{rest}"

parts = urlsplit(dsn)
if parts.scheme not in {"postgresql", "postgres"}:
    raise SystemExit(f"Esquema no soportado para psql: {parts.scheme}")

db_name = parts.path.lstrip("/")
if not db_name:
    raise SystemExit("DSN inválido: falta nombre de base de datos.")

admin_dsn = urlunsplit((parts.scheme, parts.netloc, "/postgres", parts.query, ""))
print(db_name)
print(admin_dsn)
PY
)"

DB_NAME="$(printf '%s\n' "$parse_result" | sed -n '1p')"
ADMIN_DSN="$(printf '%s\n' "$parse_result" | sed -n '2p')"

if [ -z "$DB_NAME" ] || [ -z "$ADMIN_DSN" ]; then
  echo "No se pudo obtener DB_NAME y ADMIN_DSN desde SAVH_ETL_PG_DSN." >&2
  exit 1
fi

echo "Reiniciando base de datos: $DB_NAME"
psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 -v db_name="$DB_NAME" <<'SQL'
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = :'db_name'
  AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS :"db_name";
CREATE DATABASE :"db_name";
SQL

echo "Base de datos '$DB_NAME' recreada correctamente."
