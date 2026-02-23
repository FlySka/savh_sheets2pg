
# savh-sheets2pg

ETL one-shot para cargar la BD de SAVH desde un archivo Excel (`.xlsx`) hacia PostgreSQL.

Incluye:
- Extracción de hojas Excel a `pandas.DataFrame`
- Transformaciones para normalizar/limpiar tablas
- Inicialización del modelo vía DDL (`src/savh_etl/sql/*.sql`)
- Carga ordenada a Postgres (respeta dependencias) + sync de secuencias `id`

## Requisitos

- Python `>= 3.10`
- Poetry (recomendado)
- PostgreSQL (accesible desde el DSN configurado)

## Instalación

```bash
poetry install
```

## Configuración (.env)

1) Copia el template:

```bash
cp .env.example .env
```

2) Edita `.env` y ajusta, como mínimo:
- `SAVH_ETL_EXCEL_PATH`
- `SAVH_ETL_PG_DSN`

Notas:
- `SAVH_ETL_EXCLUDE_SHEETS` debe ser un JSON array válido (ej: `[]` o `["LOG_VENTAS"]`).
- Hojas cuyo nombre comienza con `_` se ignoran siempre (aunque no estén en exclude).

## Uso (CLI)

El proyecto expone el comando `savh-etl` (Typer).

### Ver configuración efectiva

```bash
poetry run savh-etl config
```

### Listar hojas del Excel

```bash
poetry run savh-etl list --excel-path ./data/BD_SAVH.xlsx
```

### Ejecutar el pipeline

Carga end-to-end (extract → transform → load):

```bash
poetry run savh-etl run --excel-path ./data/BD_SAVH.xlsx --pg-dsn "postgresql+psycopg://user:pass@localhost:5432/savh"
```

Dry-run (no escribe en Postgres; solo extrae/transforma y loguea):

```bash
poetry run savh-etl run --dry-run
```

Solo DDL (reset + DDL, sin insertar filas):

```bash
poetry run savh-etl run --ddl-only
```

Excluir hojas desde CLI (JSON array):

```bash
poetry run savh-etl run --exclude '["LOG_VENTAS","LOG_EGRESOS"]'
```

Opciones útiles:
- `--schema`: cambia el schema **core** (los schemas `ingest/audit` vienen de `.env` o defaults).
- `--load-mode`: `drop_create` (recrea schemas) o `truncate` (trunca tablas existentes).
- `--log-level`: `DEBUG|INFO|WARNING|ERROR`
- `--debug-table-extract`, `--debug-table-transform`: imprime un preview y esquema de una tabla específica.

## Cómo funciona la carga a Postgres

1) Reset según `SAVH_ETL_LOAD_MODE`:
   - `drop_create`: dropea schemas `core/ingest/audit` (CASCADE) y vuelve a crear vía DDL.
   - `truncate`: trunca tablas existentes (RESTART IDENTITY CASCADE). Si no hay tablas, corre DDL como bootstrap.

2) DDL pre-load (si corresponde):
   - `src/savh_etl/sql/01_schema.sql`
   - `src/savh_etl/sql/02_tables.sql`
   - `src/savh_etl/sql/03_constraints.sql`

3) Inserción:
   - Ordenada por un plan de dependencias (ver `src/savh_etl/load/load_strategy.py`).
   - Alinea columnas DataFrame ↔ DB; por defecto puede dropear columnas extra y falla si faltan columnas requeridas.

4) Sync de secuencias/identity para columnas `id`.

5) DDL post-load (si se bootstrappeó DDL):
   - `src/savh_etl/sql/04_foreign_keys.sql`
   - `src/savh_etl/sql/05_indexes.sql`
   - `src/savh_etl/sql/06_comments.sql`
   - `src/savh_etl/sql/07_triggers.sql`

## Variables de entorno principales

Las variables recomendadas están documentadas en `.env.example`.

Adicionales (opcionales):
- `SAVH_ETL_DEBUG_TABLE_EXTRACT`: nombre de tabla a inspeccionar luego de extract.
- `SAVH_ETL_DEBUG_TABLE_TRANSFORM`: nombre de tabla a inspeccionar luego de transform.
- `SAVH_ETL_DDL_ONLY`: alias legacy de `SAVH_ETL_ONLY_DDL`.
- `DATABASE_URL` / `POSTGRES_DSN`: alternativas a `SAVH_ETL_PG_DSN` (solo si invocas `savh_etl.load.load` directamente).

## Tests (opcional)

```bash
poetry run pytest
```

## Notas

- Aunque el texto del CLI menciona Google Sheets, actualmente la extracción implementada usa Excel (`src/savh_etl/io/excel_client.py`).
