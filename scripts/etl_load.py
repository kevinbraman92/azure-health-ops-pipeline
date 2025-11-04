"""
Blob → Staging → MERGE → Azure SQL (idempotent upserts).

This ETL reads CSVs from Azure Blob Storage into staging tables,
then executes stored procedures that MERGE into final tables
to achieve idempotent, deduplicated loads.
"""

import os
import pandas as pd
import time
from typing import Dict, Optional
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from etl_utils import (
    build_sqlalchemy_url,
    read_csv_from_blob,
    _split_single_column_csv,   
)


# Load env
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "config", ".env"))

# SQL connection
server   = os.environ["AZSQL_SERVER"]
database = os.environ["AZSQL_DB"]
username = os.environ["AZSQL_USER"]
password = os.environ["AZSQL_PASSWORD"]

conn_url = build_sqlalchemy_url(server, database, username, password)
engine = create_engine(conn_url, fast_executemany=True)

# Blob config
container = os.environ.get("AZURE_STORAGE_CONTAINER", "landing")

def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _coerce_types(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Enforce explicit column types on a DataFrame prior to load/merge.
    See the earlier documented version; unchanged behavior.
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
            out[col] = out[col].astype(str).str.strip()
    return out


def truncate_staging(conn) -> None:
    """
    Truncate all staging tables to ensure a clean, repeatable load.

    Args:
        conn: An active SQLAlchemy Connection (inside a transaction).

    Returns:
        None. Staging tables dbo.StgProvider / dbo.StgPatient / dbo.StgClaim are truncated.
    """
    conn.execute(text("TRUNCATE TABLE dbo.StgClaim"))
    conn.execute(text("TRUNCATE TABLE dbo.StgPatient"))
    conn.execute(text("TRUNCATE TABLE dbo.StgProvider"))


def load_staging(table_name: str, df: pd.DataFrame) -> None:
    """
    Bulk insert a DataFrame into the specified staging table.

    Args:
        table_name: Staging table name without schema, e.g., 'StgProvider'.
        df: DataFrame to insert; must match staging schema.

    Raises:
        ValueError: If DataFrame is empty.
        SQLAlchemyError: If insert fails.
    """
    if df is None or df.empty:
        raise ValueError(f"Refusing to load empty DataFrame into {table_name}.")
    df.to_sql(table_name, engine, schema="dbo", if_exists="append", index=False)


def exec_proc(proc_name: str) -> None:
    """
    Execute a stored procedure with no parameters.

    Args:
        proc_name: Name of the stored procedure, e.g., 'dbo.sp_upsert_provider'.

    Returns:
        None. Raises on failure.
    """
    with engine.begin() as conn:
        conn.execute(text(f"EXEC {proc_name}"))


def start_run() -> int:
    """
    Create a new ETL_Run row and return its RunID using OUTPUT inserted.RunID.
    This avoids batch/NOCOUNT issues that can cause ResourceClosedError.
    """
    with engine.begin() as conn:
        run_id = conn.execute(
            text("INSERT INTO dbo.ETL_Run OUTPUT inserted.RunID DEFAULT VALUES;")
        ).scalar_one()
    return int(run_id)



def update_run_counts(run_id: int,
                      stg_counts: Dict[str, int],
                      final_counts: Dict[str, int],
                      reject_total: int) -> None:
    """
    Update the ETL_Run row with staging/final counts and reject total.

    Args:
        run_id: The RunID to update.
        stg_counts: Dict with keys 'provider','patient','claim' for staging row counts.
        final_counts: Dict with keys 'provider','patient','claim' for final row counts.
        reject_total: Total rejects captured for this run.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE dbo.ETL_Run
               SET StgProviderCount = :sp,
                   StgPatientCount  = :st,
                   StgClaimCount    = :sc,
                   ProviderCount    = :fp,
                   PatientCount     = :ft,
                   ClaimCount       = :fc,
                   RejectTotal      = :rj
             WHERE RunID = :rid;
        """), dict(
            sp=stg_counts.get("provider", 0),
            st=stg_counts.get("patient", 0),
            sc=stg_counts.get("claim", 0),
            fp=final_counts.get("provider", 0),
            ft=final_counts.get("patient", 0),
            fc=final_counts.get("claim", 0),
            rj=reject_total,
            rid=run_id
        ))


def finish_run(run_id: int, status: str) -> None:
    """
    Mark the ETL run as finished.

    Args:
        run_id: The RunID to close out.
        status: 'SUCCESS' | 'FAILED' | 'PARTIAL'
    """
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE dbo.ETL_Run
               SET EndedAt = SYSUTCDATETIME(),
                   Status  = :st
             WHERE RunID = :rid;
        """), dict(st=status, rid=run_id))



def main() -> None:
    """
    Orchestrate idempotent upserts:
      1) Read CSVs from Blob (with robust parsing).
      2) Coerce dtypes.
      3) Truncate staging.
      4) Load staging tables.
      5) Execute MERGE procs for Provider, Patient, Claim.
      6) Print verification.

    Notes:
      - claims.csv must use natural-key columns to resolve FKs:
        PatientFirstName, PatientLastName, PatientBirthDate, ProviderName, ProviderRegion, ...
    """
    run_id = start_run()
    try:
        # 1) Read CSVs from Blob, robust to BOM/single-column cases
        providers = read_csv_from_blob(container, "providers.csv", sep=",", encoding="utf-8-sig")
        patients  = read_csv_from_blob(container, "patients.csv",  sep=",", encoding="utf-8-sig")
        claims    = read_csv_from_blob(container, "claims.csv",    sep=",", encoding="utf-8-sig")

        providers.columns = [c.strip() for c in providers.columns]
        patients.columns  = [c.strip() for c in patients.columns]
        claims.columns    = [c.strip() for c in claims.columns]

        if len(providers.columns) == 1:
            providers = _split_single_column_csv(providers, ["Name","Region","Specialty"])
        if len(patients.columns) == 1:
            patients = _split_single_column_csv(patients, ["FirstName","LastName","BirthDate","Gender"])
        if len(claims.columns) == 1:
            claims = _split_single_column_csv(
                claims,
                ["PatientFirstName","PatientLastName","PatientBirthDate","ProviderName","ProviderRegion",
                "AmountBilled","AmountPaid","Status","DateSubmitted","DatePaid"]
            )

        # 2) Coerce types
        providers = _coerce_types(providers, {
            "Name":"str", "Region":"str", "Specialty":"str"
        })
        patients  = _coerce_types(patients, {
            "FirstName":"str", "LastName":"str", "BirthDate":"datetime64[ns]", "Gender":"str"
        })
        claims    = _coerce_types(claims, {
            "PatientFirstName":"str", "PatientLastName":"str", "PatientBirthDate":"datetime64[ns]",
            "ProviderName":"str", "ProviderRegion":"str",
            "AmountBilled":"float", "AmountPaid":"float",
            "Status":"str", "DateSubmitted":"datetime64[ns]", "DatePaid":"datetime64[ns]"
        })

            # 3) Truncate staging and load staging tables
        with engine.begin() as conn:
            truncate_staging(conn)

        load_staging("StgProvider", providers[["Name","Region","Specialty"]])
        load_staging("StgPatient",  patients[["FirstName","LastName","BirthDate","Gender"]])
        load_staging("StgClaim",    claims[[
            "PatientFirstName","PatientLastName","PatientBirthDate",
            "ProviderName","ProviderRegion",
            "AmountBilled","AmountPaid","Status","DateSubmitted","DatePaid"
        ]])

        with engine.begin() as conn:
            stg_counts = {
                "provider": conn.execute(text("SELECT COUNT(*) FROM dbo.StgProvider")).scalar_one(),
                "patient" : conn.execute(text("SELECT COUNT(*) FROM dbo.StgPatient")).scalar_one(),
                "claim"   : conn.execute(text("SELECT COUNT(*) FROM dbo.StgClaim")).scalar_one(),
            }

        # 4) Execute MERGE procedures (provider, patient)
        exec_proc("dbo.sp_upsert_provider")
        exec_proc("dbo.sp_upsert_patient")

        # 5) Execute MERGE for claims, tagging rejects with this RunID
        with engine.begin() as conn:
            conn.execute(text("EXEC dbo.sp_upsert_claim @RunID=:rid"), {"rid": run_id})

        # 6) Final counts + rejects for this run
        with engine.begin() as conn:
            final_counts = {
                "provider": conn.execute(text("SELECT COUNT(*) FROM dbo.Provider")).scalar_one(),
                "patient" : conn.execute(text("SELECT COUNT(*) FROM dbo.Patient")).scalar_one(),
                "claim"   : conn.execute(text("SELECT COUNT(*) FROM dbo.Claim")).scalar_one(),
            }
            reject_total = conn.execute(
                text("SELECT COUNT(*) FROM dbo.Reject_Claim WHERE RunID = :rid"),
                {"rid": run_id}
            ).scalar_one()

        update_run_counts(run_id, stg_counts, final_counts, int(reject_total))
        finish_run(run_id, "SUCCESS")

        print(f"Run {run_id} complete. Final Claim rows: {final_counts['claim']}. Rejects this run: {reject_total}")
    except Exception as e:
        finish_run(run_id, "FAILED")
        raise


if __name__ == "__main__":
    main()
