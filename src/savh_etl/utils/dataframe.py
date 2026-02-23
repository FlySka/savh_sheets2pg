import pandas as pd

def dataframe_dtypes_str(df: pd.DataFrame) -> str:
    """Genera un string con los tipos de datos por columna.

    Args:
        df (pd.DataFrame): DataFrame a inspeccionar.

    Returns:
        str: Texto con dtypes por columna.
    """
    return df.dtypes.astype("string").to_string()

def dataframe_schema_report(df: pd.DataFrame) -> str:
    """Arma un reporte por columna con dtype, non-null y nulls.

    Args:
        df (pd.DataFrame): DataFrame a inspeccionar.

    Returns:
        str: Reporte tabular como string.
    """
    rep = pd.DataFrame(
        {
            "dtype": df.dtypes.astype("string"),
            "non_null": df.notna().sum(),
            "nulls": df.isna().sum(),
        }
    )
    return rep.to_string()