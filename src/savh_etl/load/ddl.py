# load/ddl.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SQLStmt:
    sql: str
    start_line: int

def _split_sql_statements(sql: str) -> list[SQLStmt]:
    """Divide un SQL en sentencias ejecutables.

    Maneja comillas simples y dobles, comentarios de linea `--`, comentarios
    de bloque `/* ... */` y bloques dollar-quoted `$$ ... $$` o `$tag$ ... $tag$`.

    Args:
      sql: Texto SQL a dividir.

    Returns:
      Lista de sentencias SQL limpias, sin comentarios ni espacios innecesarios.
    """
    sql = sql.replace("\ufeff", "")  # BOM
    out: list[SQLStmt] = []
    buf: list[str] = []

    in_sq = False
    in_dq = False
    in_line_comment = False
    in_block_comment = False
    dollar_tag: Optional[str] = None

    line = 1
    stmt_start_line = 1
    started = False

    def flush():
        nonlocal started
        s = "".join(buf).strip()
        if s and s.strip().strip(";"):
            out.append(SQLStmt(sql=s, start_line=stmt_start_line))
        buf.clear()
        started = False

    i = 0
    n = len(sql)

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        if ch == "\n":
            line += 1

        if not started:
            # tomamos como inicio cuando entra el primer char (incluye whitespace/comentario)
            stmt_start_line = line
            started = True

        # --- line comments
        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        # --- block comments
        if in_block_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        # --- inside dollar-quoted
        if dollar_tag is not None:
            buf.append(ch)
            if ch == "$" and sql.startswith(dollar_tag, i):
                for k in range(1, len(dollar_tag)):
                    buf.append(sql[i + k])
                i += len(dollar_tag)
                dollar_tag = None
                continue
            i += 1
            continue

        # --- start comment?
        if not in_sq and not in_dq:
            if ch == "-" and nxt == "-":
                buf.append(ch); buf.append(nxt)
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                buf.append(ch); buf.append(nxt)
                in_block_comment = True
                i += 2
                continue

        # --- quotes
        if ch == "'" and not in_dq:
            buf.append(ch)
            if in_sq:
                if nxt == "'":
                    buf.append(nxt)
                    i += 2
                    continue
                in_sq = False
            else:
                in_sq = True
            i += 1
            continue

        if ch == '"' and not in_sq:
            buf.append(ch)
            in_dq = not in_dq
            i += 1
            continue

        # --- start dollar-quote?
        if ch == "$" and not in_sq and not in_dq:
            j = i + 1
            while j < n and sql[j] != "$" and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            if j < n and sql[j] == "$":
                tag = sql[i : j + 1]  # $$ or $tag$
                dollar_tag = tag
                buf.append(ch)
                for k in range(i + 1, j + 1):
                    buf.append(sql[k])
                i = j + 1
                continue

        # --- delimiter
        if ch == ";" and not in_sq and not in_dq:
            buf.append(ch)
            flush()
            i += 1
            continue

        buf.append(ch)
        i += 1

    flush()
    return out


@dataclass(frozen=True)
class DDLRunner:
    sql_dir: Path
    ddl_order: Sequence[str]
    fail_fast: bool = True
    skip_missing: bool = False

    def run(self, conn, *, autocommit: bool = False) -> None:
        """Ejecuta archivos DDL en orden.

        Args:
          conn: Conexion (psycopg3 o similar) con `cursor`, `commit` y `rollback`.
          autocommit: Si True, habilita autocommit durante la ejecucion.

        Raises:
          FileNotFoundError: Si `sql_dir` no existe o falta un archivo requerido
            y `skip_missing` es False.
          Exception: Propaga errores al ejecutar DDL cuando `fail_fast` es True.
        """
        sql_dir = self.sql_dir
        if not sql_dir.exists():
            raise FileNotFoundError(f"SQL dir not found: {sql_dir}")

        # Some users like autocommit for DDL; default False so we control commits per file.
        prev_autocommit = getattr(conn, "autocommit", None)
        if prev_autocommit is not None:
            conn.autocommit = autocommit

        try:
            for fname in self.ddl_order:
                fpath = sql_dir / fname
                if not fpath.exists():
                    msg = f"DDL file missing: {fpath}"
                    if self.skip_missing:
                        logger.warning(msg + " (skipped)")
                        continue
                    raise FileNotFoundError(msg)

                logger.info("Running DDL: %s", fpath.name)
                self._run_file(conn, fpath)
                logger.info("OK: %s", fpath.name)

        except Exception:
            # rollback if possible
            if hasattr(conn, "rollback"):
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            if prev_autocommit is not None:
                conn.autocommit = prev_autocommit

    def _run_file(self, conn, fpath: Path) -> None:
        """Ejecuta todas las sentencias DDL de un archivo.

        Args:
          conn: Conexion con `cursor` y `commit`.
          fpath: Ruta del archivo SQL a ejecutar.

        Raises:
          Exception: Si alguna sentencia falla y `fail_fast` es True.
        """
        sql = fpath.read_text(encoding="utf-8")
        statements = _split_sql_statements(sql)

        if not statements:
            logger.info("Empty DDL file: %s (skipped)", fpath.name)
            return

        is_autocommit = bool(getattr(conn, "autocommit", False))

        with conn.cursor() as cur:
            for idx, st in enumerate(statements, start=1):
                stmt_clean = st.sql.strip()
                if not stmt_clean:
                    continue

                # ignora meta comandos psql si aparecen
                if stmt_clean.startswith("\\"):
                    logger.warning(
                        "Skipping psql meta command in %s (stmt %d line %d): %s",
                        fpath.name, idx, st.start_line, stmt_clean[:120],
                    )
                    continue

                try:
                    if (not self.fail_fast) and (not is_autocommit):
                        # ✅ Mejora 2: SAVEPOINT por sentencia para poder continuar en Postgres
                        sp = f"sp_{idx}"
                        cur.execute(f"SAVEPOINT {sp};")
                        try:
                            cur.execute(stmt_clean.replace("%", "%%"))
                            cur.execute(f"RELEASE SAVEPOINT {sp};")
                        except Exception:
                            cur.execute(f"ROLLBACK TO SAVEPOINT {sp};")
                            cur.execute(f"RELEASE SAVEPOINT {sp};")
                            logger.exception(
                                "DDL error in %s (stmt %d, start line %d). Continuing...",
                                fpath.name, idx, st.start_line,
                            )
                            continue
                    else:
                        # psycopg (y otros DBAPIs con paramstyle '%') interpretan %I/%L en strings
                        # como placeholders, aunque sean parte de `format()` de Postgres.
                        cur.execute(stmt_clean.replace("%", "%%"))

                except Exception:
                    # ✅ Mejora 3: log con stmt # + línea
                    logger.error(
                        "DDL error in %s (stmt %d, start line %d)\n--- SQL (head) ---\n%s",
                        fpath.name, idx, st.start_line, stmt_clean[:2000],
                    )
                    raise

        # commit solo si NO estamos en autocommit
        if hasattr(conn, "commit") and not is_autocommit:
            conn.commit()


def run_ddl(conn, sql_dir: str | Path, ddl_order: Sequence[str]) -> None:
    """Ejecuta DDL desde un directorio.

    Args:
      conn: Conexion (psycopg3 o similar).
      sql_dir: Directorio con archivos SQL.
      ddl_order: Orden de ejecucion de archivos.
    """
    runner = DDLRunner(Path(sql_dir), ddl_order=ddl_order)
    runner.run(conn)


def reset_schema(conn, schema_name: str, *, cascade: bool = True, recreate: bool = True) -> None:
    """Elimina y opcionalmente recrea un schema.

    Usar con cuidado si `schema_name` es "public".

    Args:
      conn: Conexion con `cursor` y `commit`.
      schema_name: Nombre del schema.
      cascade: Si True, usa `CASCADE` al eliminar.
      recreate: Si True, crea el schema luego de eliminarlo.
    """
    drop_sql = f'DROP SCHEMA IF EXISTS "{schema_name}" {"CASCADE" if cascade else ""};'
    create_sql = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";'

    with conn.cursor() as cur:
        cur.execute(drop_sql)
        if recreate:
            cur.execute(create_sql)
    if hasattr(conn, "commit"):
        conn.commit()


# =============================================================================
# SQLAlchemy helpers (usados por el loader one-shot)
# =============================================================================


@dataclass(frozen=True)
class ColumnMeta:
    """Metadata mínima de una columna en Postgres (para validación de loads)."""

    name: str
    is_nullable: bool
    has_default: bool
    is_identity: bool


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def get_table_columns_meta(engine, schema: str, table: str) -> list[ColumnMeta]:
    """Obtiene metadata de columnas desde information_schema.

    Args:
      engine: SQLAlchemy Engine.
      schema: Schema (ej: core).
      table: Tabla (sin schema).

    Returns:
      Lista de ColumnMeta en orden de aparición (ordinal_position).
    """
    from sqlalchemy import text

    sql = """
    SELECT
      column_name,
      is_nullable,
      column_default,
      is_identity
    FROM information_schema.columns
    WHERE table_schema = :schema AND table_name = :table
    ORDER BY ordinal_position;
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"schema": schema, "table": table}).all()

    out: list[ColumnMeta] = []
    for column_name, is_nullable, column_default, is_identity in rows:
        out.append(
            ColumnMeta(
                name=str(column_name),
                is_nullable=str(is_nullable).upper() == "YES",
                has_default=column_default is not None,
                is_identity=str(is_identity).upper() == "YES",
            )
        )
    return out


def run_sql_files(
    engine,
    sql_dir: str | Path,
    ddl_files: Sequence[str],
    *,
    fail_fast: bool = True,
    ignore_sqlstates: set[str] | None = None,
) -> None:
    """Ejecuta archivos .sql (DDL) en orden, usando SQLAlchemy.

    Args:
      engine: SQLAlchemy Engine.
      sql_dir: Directorio donde viven los .sql.
      ddl_files: Lista de nombres de archivos a ejecutar.
      fail_fast: Si True, aborta ante el primer error no ignorado.
      ignore_sqlstates: SQLSTATEs a ignorar (por ejemplo {"42710"} duplicate_object).
    """
    from sqlalchemy.exc import DBAPIError

    sql_dir = Path(sql_dir)
    if not sql_dir.exists():
        raise FileNotFoundError(f"SQL dir not found: {sql_dir}")

    ignore_sqlstates = ignore_sqlstates or set()
    paramstyle = getattr(getattr(engine, "dialect", None), "paramstyle", None)
    escape_pct = paramstyle in {"pyformat", "format"}

    for fname in ddl_files:
        fpath = sql_dir / fname
        if not fpath.exists():
            raise FileNotFoundError(f"DDL file missing: {fpath}")

        sql = fpath.read_text(encoding="utf-8")
        statements = _split_sql_statements(sql)

        if not statements:
            logger.info("Empty DDL file: %s (skipped)", fpath.name)
            continue

        logger.info("Running DDL: %s", fpath.name)
        with engine.begin() as conn:
            for idx, st in enumerate(statements, start=1):
                stmt_clean = st.sql.strip()
                if not stmt_clean:
                    continue

                if stmt_clean.startswith("\\"):
                    logger.warning(
                        "Skipping psql meta command in %s (stmt %d line %d): %s",
                        fpath.name,
                        idx,
                        st.start_line,
                        stmt_clean[:120],
                    )
                    continue

                stmt_to_exec = stmt_clean.replace("%", "%%") if escape_pct else stmt_clean

                try:
                    conn.exec_driver_sql(stmt_to_exec)
                except DBAPIError as e:
                    sqlstate = getattr(getattr(e, "orig", None), "sqlstate", None)
                    if sqlstate in ignore_sqlstates:
                        logger.warning(
                            "Ignoring DDL error in %s (stmt %d line %d) sqlstate=%s",
                            fpath.name,
                            idx,
                            st.start_line,
                            sqlstate,
                        )
                        continue

                    logger.error(
                        "DDL error in %s (stmt %d, start line %d)\n--- SQL (head) ---\n%s",
                        fpath.name,
                        idx,
                        st.start_line,
                        stmt_clean[:2000],
                    )
                    if fail_fast:
                        raise
                except Exception:
                    logger.error(
                        "DDL error in %s (stmt %d, start line %d)\n--- SQL (head) ---\n%s",
                        fpath.name,
                        idx,
                        st.start_line,
                        stmt_clean[:2000],
                    )
                    if fail_fast:
                        raise


def sync_identity_sequences(engine, schema: str, table: str, *, id_column: str = "id") -> None:
    """Ajusta la secuencia asociada a una columna serial/identity al MAX(id).

    Útil cuando cargas ids explícitos (one-shot) en tablas con identity/serial.
    """
    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError

    qualified = f"{schema}.{table}"
    with engine.begin() as conn:
        # Evita errores cuando la tabla no tiene la columna esperada (ej: PK = tercero_id)
        col_exists = conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = :schema
                  AND table_name = :table
                  AND column_name = :col
                LIMIT 1;
                """
            ),
            {"schema": schema, "table": table, "col": id_column},
        ).scalar()
        if not col_exists:
            return

        seq = conn.execute(
            text("SELECT pg_get_serial_sequence(:tbl, :col);"),
            {"tbl": qualified, "col": id_column},
        ).scalar()
        if not seq:
            return

        max_sql = (
            f"SELECT MAX({_quote_ident(id_column)}) "
            f"FROM {_quote_ident(schema)}.{_quote_ident(table)};"
        )
        max_id = conn.execute(text(max_sql)).scalar()

        try:
            if max_id is None:
                conn.execute(
                    text("SELECT setval(CAST(:seq AS regclass), 1, false);"),
                    {"seq": seq},
                )
            else:
                conn.execute(
                    text("SELECT setval(CAST(:seq AS regclass), :v, true);"),
                    {"seq": seq, "v": int(max_id)},
                )
        except DBAPIError as e:
            # Si el usuario no tiene privilegios sobre la secuencia, no bloqueamos el load.
            sqlstate = getattr(getattr(e, "orig", None), "sqlstate", None)
            if sqlstate == "42501":  # insufficient_privilege
                return
            raise
