from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from savh_etl.load.load_strategy import build_savh_load_plan
from savh_etl.transform.mapping import normalize_table_name
from savh_etl.transform.transform import transform


def _minimal_tables() -> dict[str, pd.DataFrame]:
    return {
        "CLIENTES": pd.DataFrame(
            [
                {
                    "id": "1",
                    "nombre": "canal uno",
                    "alias": "",
                    "tipo": "CANAL",
                    "rut": "11111111-1",
                    "vendedor": "savh",
                    "telefono": "",
                    "direccion": "",
                    "factura": "0",
                    "factura_despacho": "0",
                    "activo": "1",
                }
            ]
        ),
        "CAT_TIPOS_CLIENTE": pd.DataFrame(
            [
                {"id": "1", "nombre": "FINAL", "activo": "1"},
                {"id": "2", "nombre": "CANAL", "activo": "1"},
            ]
        ),
        "VENDEDORES": pd.DataFrame(
            [
                {
                    "id": "1",
                    "nombre": "savh",
                    "contrato": "COMISION",
                    "cliente_id": "",
                    "telefono": "",
                    "comision": "0.05",
                    "activo": "1",
                }
            ]
        ),
        "CONTRATOS_VENDEDOR": pd.DataFrame(
            [
                {
                    "id": "1",
                    "Column 7": "savh",
                    "tipo": "FIJO",
                    "base": "BRUTO_CON_IVA",
                    "tasa_comision": "250.0",
                    "activo": "1",
                }
            ]
        ),
        "CAT_TIPO_CONTRATO_VENDEDOR": pd.DataFrame(
            [
                {"id": "1", "tipo": "PORCENTUAL", "activo": "1"},
                {"id": "2", "tipo": "FIJO", "activo": "1"},
            ]
        ),
        "CAT_BASE_COMISION_VENDEDOR": pd.DataFrame(
            [
                {"id": "1", "base": "NETO_SIN_IVA", "activo": "1"},
                {"id": "2", "base": "BRUTO_CON_IVA", "activo": "1"},
            ]
        ),
        "PROVEEDORES": pd.DataFrame(
            columns=["id", "nombre", "rut", "telefono", "direccion", "activo"]
        ),
        "TRABAJADORES": pd.DataFrame(
            columns=["id", "nombre", "apellido", "cargo", "rut", "telefono", "activo"]
        ),
        "DESTINATARIOS": pd.DataFrame(
            columns=["id", "cliente", "nombre", "rut", "telefono", "direccion", "activo"]
        ),
        "CAT_VEHICULOS": pd.DataFrame(
            columns=["id", "alias", "patente", "tipo", "marca", "modelo", "anio", "activo"]
        ),
    }


class TransformNewCatalogsTest(unittest.TestCase):
    def test_transform_maps_customer_types_and_salesperson_contracts(self) -> None:
        out = transform(_minimal_tables())

        self.assertEqual(out["clientes"]["tipo_id"].iloc[0], 2)
        self.assertEqual(out["terceros_cliente"]["tipo_id"].iloc[0], 2)
        self.assertNotIn("column_7", out["contratos_vendedor"].columns)
        self.assertNotIn("vendedor", out["contratos_vendedor"].columns)
        self.assertEqual(out["contratos_vendedor"]["tipo_id"].iloc[0], 2)
        self.assertEqual(out["contratos_vendedor"]["base_id"].iloc[0], 2)
        self.assertEqual(float(out["contratos_vendedor"]["tasa_comision"].iloc[0]), 250.0)
        self.assertNotIn("comision", out["terceros_vendedor"].columns)

        expected_salesperson_id = (
            out["terceros"]
            .loc[out["terceros"]["id_old"].astype("string").str.contains("vendedores:1", na=False), "id"]
            .iloc[0]
        )
        self.assertEqual(out["contratos_vendedor"]["vendedor_id"].iloc[0], expected_salesperson_id)

    def test_transform_moves_vehicle_is_active_to_assets(self) -> None:
        tables = _minimal_tables()
        tables["CAT_VEHICULOS"] = pd.DataFrame(
            [
                {
                    "id": "10",
                    "alias": "camion 1",
                    "patente": "ABCD11",
                    "tipo": "camion",
                    "marca": "volvo",
                    "modelo": "fh",
                    "anio": "2020",
                    "activo": "0",
                }
            ]
        )

        out = transform(tables)

        self.assertIn("is_active", out["assets"].columns)
        self.assertNotIn("is_active", out["assets_vehicles"].columns)
        self.assertFalse(bool(out["assets"]["is_active"].iloc[0]))
        self.assertEqual(out["assets"]["id"].iloc[0], 10)
        self.assertEqual(out["assets_vehicles"]["activo_id"].iloc[0], 10)

    def test_legacy_customer_type_sheet_name_normalizes_to_new_key(self) -> None:
        self.assertEqual(normalize_table_name("CAT_TIPOS_CLIENTES"), "dim_tipos_cliente")


class LoadPlanContractsTest(unittest.TestCase):
    def test_load_plan_uses_new_df_keys(self) -> None:
        plan = build_savh_load_plan()
        specs = {spec.name: spec for spec in plan.specs}

        self.assertEqual(specs["dim_customer_types"].df_key, "dim_tipos_cliente")
        self.assertEqual(specs["dim_salesperson_contract_types"].df_key, "dim_tipo_contrato_vendedor")
        self.assertEqual(specs["dim_salesperson_commission_bases"].df_key, "dim_base_comision_vendedor")
        self.assertEqual(specs["salesperson_contracts"].df_key, "contratos_vendedor")


class SqlSchemaContractsTest(unittest.TestCase):
    def test_sql_schema_supports_salesperson_contracts(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        tables_sql = (repo_root / "src" / "savh_etl" / "sql" / "02_tables.sql").read_text(encoding="utf-8")
        constraints_sql = (repo_root / "src" / "savh_etl" / "sql" / "03_constraints.sql").read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE core.salesperson_contracts", tables_sql)
        self.assertIn("tasa_comision numeric(14,6)", tables_sql)
        self.assertNotIn("comision numeric(6,4)", tables_sql)
        self.assertIn("chk_salesperson_contracts_tasa_comision", constraints_sql)
        self.assertIn("CHECK (tasa_comision >= 0)", constraints_sql)

    def test_sql_schema_moves_is_active_from_assets_vehicles_to_assets(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        tables_sql = (repo_root / "src" / "savh_etl" / "sql" / "02_tables.sql").read_text(encoding="utf-8")

        self.assertIn(
            "CREATE TABLE core.assets (\n"
            "  id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,\n"
            "  alias varchar UNIQUE,\n"
            "  tipo_activo_id int,\n"
            "  is_active boolean DEFAULT true\n"
            ");",
            tables_sql,
        )
        self.assertIn(
            "CREATE TABLE core.assets_vehicles (\n"
            "  activo_id int PRIMARY KEY,\n"
            "  patente varchar,\n"
            "  tipo varchar,\n"
            "  marca varchar,\n"
            "  modelo varchar,\n"
            "  anio int\n"
            ");",
            tables_sql,
        )


if __name__ == "__main__":
    unittest.main()
