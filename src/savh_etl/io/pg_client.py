from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Literal, Sequence

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

IfExists = Literal["fail", "replace", "append"]


@dataclass(frozen=True)
class PostgresClient:
    """Cliente IO para PostgreSQL (ETL one-shot friendly).

    Attributes:
      dsn: DSN de conexión a PostgreSQL.
    """

    dsn: str

    @cached_property
    def _engine(self) -> Engine:
        """Construye y cachea el engine SQLAlchemy.

        Returns:
          Engine conectado a `dsn`.
        """
        # pool_pre_ping evita conexiones muertas en sesiones largas
        return create_engine(self.dsn, pool_pre_ping=True)

    def engine(self) -> Engine:
        """Retorna el engine SQLAlchemy cacheado.

        Returns:
          Engine listo para usar.
        """
        return self._engine
    
    # cerrar pool/conexiones (importante en Windows)
    def close(self) -> None:
        self._engine.dispose()

    # permite `with PostgresClient(...) as pg: ...`
    def __enter__(self) -> "PostgresClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        self.close()

    @staticmethod
    def _quote_ident(name: str) -> str:
        """Escapa identificadores SQL para PostgreSQL.

        Args:
          name: Identificador a escapar.

        Returns:
          Identificador con comillas dobles y escapes.
        """
        return '"' + name.replace('"', '""') + '"'

    def _qualify_table(self, schema: str, table_name: str) -> str:
        """Devuelve schema.tabla con quoting seguro.

        Args:
          schema: Nombre del schema.
          table_name: Nombre de la tabla.

        Returns:
          Identificador calificado con comillas.
        """
        return f"{self._quote_ident(schema)}.{self._quote_ident(table_name)}"

    def exec(self, sql: str, params: dict | None = None) -> None:
        """Ejecuta SQL dentro de una transacción.

        Args:
          sql: SQL a ejecutar.
          params: Parámetros para el SQL (si aplica).
        """
        with self.engine().begin() as conn:
            conn.execute(text(sql), params or {})

    def drop_schema(self, schema: str) -> None:
        """Elimina un schema si existe (con cascade).

        Args:
          schema: Nombre del schema.
        """
        self.exec(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')

    def create_schema(self, schema: str) -> None:
        """Crea un schema si no existe.

        Args:
          schema: Nombre del schema.
        """
        self.exec(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

    def read_sql(self, sql: str, params: dict | None = None) -> pd.DataFrame:
        """Ejecuta un SELECT y retorna un DataFrame.

        Args:
          sql: Query SQL a ejecutar.
          params: Parámetros para el query (si aplica).

        Returns:
          DataFrame con el resultado del query.
        """
        with self.engine().connect() as conn:
            return pd.read_sql_query(text(sql), conn, params=params or {})

    def read_table(
        self,
        schema: str,
        table_name: str,
        columns: Sequence[str] | None = None,
        where: str | None = None,
        params: dict | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Lee una tabla con filtros opcionales.

        Args:
          schema: Schema de la tabla.
          table_name: Nombre de la tabla.
          columns: Columnas a seleccionar (None => todas).
          where: Cláusula WHERE sin la palabra WHERE.
          params: Parámetros para la cláusula WHERE (si aplica).
          limit: Límite de filas (None => sin límite).

        Returns:
          DataFrame con el resultado.
        """
        cols_sql = "*"
        if columns:
            cols_sql = ", ".join(self._quote_ident(c) for c in columns)

        sql = f"SELECT {cols_sql} FROM {self._qualify_table(schema, table_name)}"
        if where:
            sql += f" WHERE {where}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"

        return self.read_sql(sql, params=params)

    def read_scalar(self, sql: str, params: dict | None = None) -> object | None:
        """Retorna un valor escalar (1x1).

        Args:
          sql: Query SQL a ejecutar.
          params: Parámetros para el query (si aplica).

        Returns:
          Valor escalar o None si no hay filas.
        """
        with self.engine().connect() as conn:
            return conn.execute(text(sql), params or {}).scalar()

    def table_exists(self, schema: str, table_name: str) -> bool:
        """Indica si una tabla existe en el schema.

        Args:
          schema: Nombre del schema.
          table_name: Nombre de la tabla.

        Returns:
          True si la tabla existe, False en caso contrario.
        """
        sql = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = :schema AND table_name = :table_name
        LIMIT 1;
        """
        return self.read_scalar(
            sql,
            {"schema": schema, "table_name": table_name},
        ) is not None

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        if_exists: IfExists = "fail",
        chunksize: int = 5000,
        index: bool = False,
        method: str | None = "multi",
    ) -> None:
        """Carga un DataFrame en una tabla.

        Normaliza nombres de columnas y utiliza `DataFrame.to_sql`.

        Args:
          df: DataFrame a cargar.
          table_name: Nombre de la tabla destino.
          schema: Schema de la tabla destino.
          if_exists: Comportamiento si la tabla existe.
          chunksize: Tamaño de lote para la carga.
          index: Indica si se escribe el índice del DataFrame.
          method: Método de inserción (pandas/SQLAlchemy).
        """
        if not self.table_exists(schema, table_name):
            raise RuntimeError(f"Target table not found: {schema}.{table_name}")

        df2 = df.copy()
        df2.columns = [str(c).strip() for c in df2.columns]

        with self.engine().begin() as conn:
            df2.to_sql(
                name=table_name,
                con=conn,
                schema=schema,
                if_exists=if_exists,
                index=index,
                chunksize=chunksize,
                method=method,
            )

    def reset_identity(self, schema: str, table_name: str, pk: str = "id") -> None:
        """Ajusta la secuencia/identity al MAX(pk) tras cargas con ids explícitos.

        Args:
          schema: Schema de la tabla.
          table_name: Nombre de la tabla.
          pk: Columna PK/identity.
        """
        full = self._qualify_table(schema, table_name)
        pk_ident = self._quote_ident(pk)
        sql = f"""
        SELECT setval(
            pg_get_serial_sequence(:full, :pk),
            COALESCE((SELECT MAX({pk_ident}) FROM {full}), 0),
            true
        );
        """
        self.exec(sql, {"full": full, "pk": pk})
