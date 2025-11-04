"""
Utility functions for the Azure Healthcare ETL pipeline.

This module centralizes helper logic for:
  - Building secure SQLAlchemy connection URLs for Azure SQL Database.
  - Picking an installed ODBC driver dynamically.
  - Connecting to Azure Blob Storage using the Azure SDK.
  - Reading CSVs from Blob Storage directly into pandas DataFrames.
  - Repairing malformed single-column CSVs (common with Excel/BOM exports).

All helpers are designed to be imported by ETL scripts such as `etl_load.py`.
"""

import os
from io import BytesIO
from typing import Dict, Any, List
from urllib.parse import quote_plus

import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient


def _pick_sql_driver() -> str:
    """
    Determine which Microsoft ODBC driver for SQL Server is installed.

    Prefers the latest available driver in descending order of version.
    Azure SQL requires an encrypted connection, which both Driver 17 and 18 support.

    Returns:
        str: The name of the installed driver (e.g., "ODBC Driver 18 for SQL Server").

    Raises:
        RuntimeError: If no supported SQL Server ODBC driver is installed.
    """
    drivers = [d for d in pyodbc.drivers()]
    for target in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
        if target in drivers:
            return target
    raise RuntimeError(
        f"No supported SQL Server ODBC driver found. "
        f"Installed drivers: {drivers}. Please install 'ODBC Driver 18 for SQL Server'."
    )


def build_sqlalchemy_url(server: str, database: str, username: str, password: str) -> str:
    """
    Construct a full SQLAlchemy connection string for Azure SQL Database.

    Builds a properly encoded ODBC connection string wrapped in the
    SQLAlchemy `mssql+pyodbc:///?odbc_connect=` format.

    Args:
        server (str): Azure SQL Server FQDN (e.g., 'sql-demo.database.windows.net').
        database (str): Target database name.
        username (str): SQL login username.
        password (str): SQL login password.

    Returns:
        str: A SQLAlchemy-compatible connection string with encryption enabled.

    Example:
        >>> build_sqlalchemy_url('server.database.windows.net', 'db', 'user', 'pwd')
        'mssql+pyodbc:///?odbc_connect=DRIVER%3DODBC+Driver+18...'
    """
    driver = _pick_sql_driver()
    params = (
        f"DRIVER={driver};"
        f"SERVER={server},1433;"
        f"DATABASE={database};"
        f"UID={username};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    return f"mssql+pyodbc:///?odbc_connect={quote_plus(params)}"


def get_blob_client() -> BlobServiceClient:
    """
    Create and return an Azure BlobServiceClient using the connection string
    found in the environment variable `AZURE_STORAGE_CONNECTION_STRING`.

    Returns:
        BlobServiceClient: A client that can list, upload, and download blobs.

    Raises:
        KeyError: If `AZURE_STORAGE_CONNECTION_STRING` is not set in the environment.
        azure.core.exceptions.AzureError: If connection to Blob service fails.

    Notes:
        - In production, prefer using Managed Identity or SAS tokens over
          a raw connection string for security.
        - This helper centralizes authentication logic for reuse.
    """
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    return BlobServiceClient.from_connection_string(conn_str)


def read_csv_from_blob(
    container: str,
    blob_name: str,
    **read_csv_kwargs: Dict[str, Any]
) -> pd.DataFrame:
    """
    Stream a CSV file directly from Azure Blob Storage into a pandas DataFrame.

    Downloads the blob as bytes (in-memory, no temp files) and parses it with pandas.
    The default parser settings are conservative to avoid dtype surprises and
    allow you to control coercion later in your ETL layer.

    Args:
        container (str): The name of the container holding the CSV.
        blob_name (str): The blob (file) name, e.g. 'providers.csv'.
        **read_csv_kwargs: Optional keyword arguments forwarded to `pandas.read_csv()`,
            such as `sep`, `encoding`, or `dtype`.

    Returns:
        pd.DataFrame: The parsed CSV content as a pandas DataFrame.

    Raises:
        azure.core.exceptions.ResourceNotFoundError: If the blob does not exist.
        pandas.errors.ParserError: If the file cannot be parsed as CSV.

    Example:
        >>> df = read_csv_from_blob('landing', 'providers.csv', sep=',', encoding='utf-8-sig')
        >>> df.head()
             Name          Region     Specialty
        0  North Clinic    Midwest   Primary Care
    """
    bsc = get_blob_client()
    blob = bsc.get_blob_client(container=container, blob=blob_name)

    # Download blob contents to memory
    stream = BytesIO()
    download = blob.download_blob()
    download.readinto(stream)
    stream.seek(0)

    # Apply safe defaults if user didn’t pass them
    if "dtype" not in read_csv_kwargs:
        read_csv_kwargs["dtype"] = str
    if "na_filter" not in read_csv_kwargs:
        read_csv_kwargs["na_filter"] = True

    df = pd.read_csv(stream, **read_csv_kwargs)
    return df


def _split_single_column_csv(
    df: pd.DataFrame,
    expected_cols: List[str],
    new_names: List[str] | None = None
) -> pd.DataFrame:
    """
    Repair a malformed CSV DataFrame that was parsed as a single column.

    This situation often occurs when:
      - The file includes a BOM (Byte Order Mark) and pandas fails to detect the delimiter.
      - The file uses Excel's default CSV encoding or nonstandard delimiters.
      - The blob was uploaded with carriage-return formatting from Windows.

    This function detects the delimiter, splits the single text column into multiple
    columns, renames headers, and validates that the expected column names exist.

    Args:
        df (pd.DataFrame): The malformed DataFrame (one column containing comma-separated text).
        expected_cols (List[str]): The list of expected column names, in order.
        new_names (List[str], optional): Replacement names to assign after splitting.
            Defaults to using `expected_cols`.

    Returns:
        pd.DataFrame: A cleaned DataFrame with properly separated and named columns.

    Raises:
        ValueError: If expected columns cannot be found after splitting.

    Example:
        >>> malformed = pd.DataFrame({'Name,Region,Specialty': [
        ...     'North Clinic,Midwest,Primary Care',
        ...     'Sunrise Hospital,South,Cardiology'
        ... ]})
        >>> clean = _split_single_column_csv(malformed, ['Name','Region','Specialty'])
        >>> clean.columns.tolist()
        ['Name', 'Region', 'Specialty']
    """
    if len(df.columns) == 1 and any(ch in df.columns[0] for ch in [",", ";", "|"]):
        # Identify the delimiter
        sep = ","
        if ";" in df.columns[0]:
            sep = ";"
        elif "|" in df.columns[0]:
            sep = "|"

        # Split the header and rows
        header_parts = [p.strip() for p in df.columns[0].split(sep)]
        df = df[df.columns[0]].str.split(sep, expand=True)
        df.columns = header_parts

    # Strip whitespace
    df.columns = [c.strip() for c in df.columns]

    # Validate columns
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}. Found: {df.columns.tolist()}")

    # Optionally rename columns
    if new_names:
        df = df[expected_cols].rename(columns=dict(zip(expected_cols, new_names)))
    else:
        df = df[expected_cols]

    return df

def _coerce_types(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Enforce explicit column types on a DataFrame prior to database load.

    This helps avoid implicit dtype inference that can cause load errors or
    inconsistent schema (e.g., floats interpreted as strings, dates as objects).

    Args:
        df: The input DataFrame to coerce.
        mapping: A dict mapping column name to target dtype. Supported values:
            - "str":           Coerces to string and strips whitespace.
            - "float":         Coerces to numeric (float) with errors='coerce'.
            - "int":           Coerces to nullable integer (Int64) with errors='coerce'.
            - "datetime64[ns]": Parses to pandas datetime64[ns] (errors='coerce').

    Returns:
        A copy of the DataFrame with the specified columns coerced to target types.
        Columns not present in the DataFrame are ignored.

    Notes:
        - Datetime coercion uses `utc=False` to keep naive timestamps by design.
        - Numeric coercion uses `errors='coerce'` so invalid values become NaN.
          Handle NaN upstream if those are not acceptable for your target table.
    """
    out = df.copy()
    for col, target in mapping.items():
        if col not in out.columns:
            continue
        if target.startswith("datetime"):
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=False)
        elif target == "float":
            out[col] = pd.to_numeric(out[col], errors="coerce")
        elif target == "int":
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
        elif target == "str":
            out[col] = out[col].astype(str).str.strip()
        else:
            # Fallback: cast to string unless you intentionally add more handlers above.
            out[col] = out[col].astype(str).str.strip()
    return out
