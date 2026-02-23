"""Cliente IO para Excel.

Lee hojas (worksheets) desde un archivo XLSX y las devuelve como DataFrames.

Decisión de diseño:
  - Se lee con dtype=str para evitar inferencias sorpresivas (fechas/números).
  - El tipado/casteo correcto se implementa en la capa transform.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass(frozen=True)
class ExcelClient:
    """Wrapper simple para lectura de hojas Excel.

    Attributes:
      path: Ruta al archivo XLSX.
    """

    path: str

    def list_sheets(self) -> List[str]:
        """Lista nombres de hojas dentro del libro Excel.

        Returns:
          Lista de nombres de hojas.
        """
        xls = pd.ExcelFile(self.path)
        return list(xls.sheet_names)

    def read_sheet(self, sheet_name: str) -> pd.DataFrame:
        """Lee una hoja y retorna un DataFrame.

        Args:
          sheet_name: Nombre de la hoja.

        Returns:
          DataFrame con todas las celdas leídas como string.
        """
        # dtype=str hace estable la extracción; el tipado va después.
        return pd.read_excel(self.path, sheet_name=sheet_name, dtype=str)
