from __future__ import annotations

from pathlib import Path
from prefect import task
from etl.utils import get_connection


# ---------------------------
# Helpers: metadata & listing
# ---------------------------

def _ensure_metadata(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS meta;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS meta.processed_batches (
            dt VARCHAR PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def _list_raw_batches(raw_root: str) -> list[str]:
    root = Path(raw_root)
    if not root.exists():
        return []
    dts: list[str] = []
    for p in root.iterdir():
        if p.is_dir() and p.name.startswith("dt="):
            dts.append(p.name.replace("dt=", ""))
    return sorted(dts)


def _table_exists(con, schema: str, table: str) -> bool:
    q = """
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = ? AND table_name = ?
    """
    return con.execute(q, [schema, table]).fetchone()[0] > 0


def _get_table_columns(con, schema: str, table: str) -> list[str]:
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = ? AND table_name = ?
    ORDER BY ordinal_position
    """
    return [r[0] for r in con.execute(q, [schema, table]).fetchall()]


def _get_parquet_columns(con, parquet_path: str) -> set[str]:
    # DESCRIBE on a SELECT is a handy way to get columns DuckDB sees
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").fetchall()
    return {r[0] for r in rows}


def _get_csv_columns(con, csv_path: str) -> set[str]:
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_csv_auto('{csv_path}', header=True)"
    ).fetchall()
    return {r[0] for r in rows}


def _insert_aligned(
    con,
    schema: str,
    table: str,
    source_kind: str,  # "parquet" | "csv"
    source_path: str,
    dt: str,
) -> None:
    """
    Inserisce nel target solo le colonne che il target si aspetta.
    - Se il file ha colonne in più: ignorate.
    - Se il file ha colonne in meno: inseriamo NULL per quelle colonne.
    - ingest_dt: sempre valorizzata con literal dt.
    """
    target_cols = _get_table_columns(con, schema, table)

    if source_kind == "parquet":
        file_cols = _get_parquet_columns(con, source_path)
        from_sql = f"read_parquet('{source_path}')"
    else:
        file_cols = _get_csv_columns(con, source_path)
        from_sql = f"read_csv_auto('{source_path}', header=True)"

    select_exprs: list[str] = []
    insert_cols_sql = ", ".join([f'"{c}"' for c in target_cols])

    for col in target_cols:
        if col == "ingest_dt":
            select_exprs.append(f"'{dt}' AS ingest_dt")
        elif col in file_cols:
            select_exprs.append(f'"{col}"')
        else:
            select_exprs.append(f'NULL AS "{col}"')

    select_sql = ", ".join(select_exprs)

    con.execute(f"""
        INSERT INTO {schema}.{table} ({insert_cols_sql})
        SELECT {select_sql}
        FROM {from_sql};
    """)


# ---------------------------
# Task: Bronze incremental
# ---------------------------

@task
def load_bronze_f1_incremental(raw_root: str = "data_lake/raw") -> list[str]:
    """
    Fase 2 Bronze:
    - legge batch organizzati in raw_root/dt=YYYY-MM-DD/
    - processa solo i dt non presenti in meta.processed_batches
    - carica i dataset in schema bronze (una tabella per file) aggiungendo ingest_dt
    - gestisce mismatch di colonne tra batch
    """
    con = get_connection(read_only=False)

    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    _ensure_metadata(con)

    all_dts = _list_raw_batches(raw_root)
    if not all_dts:
        con.close()
        print("[INFO] Nessun batch trovato in:", raw_root)
        return []

    processed = {r[0] for r in con.execute("SELECT dt FROM meta.processed_batches").fetchall()}
    to_process = [dt for dt in all_dts if dt not in processed]

    print("Found dt:", all_dts)
    print("Already processed dt:", sorted(processed))
    print("To process:", to_process)

    for dt in to_process:
        batch_dir = Path(raw_root) / f"dt={dt}"

        parquet_files = sorted(batch_dir.glob("*.parquet"))
        csv_files = sorted(batch_dir.glob("*.csv"))

        # Preferiamo parquet; se non ci sono, usiamo csv
        if parquet_files:
            files = [(f, "parquet") for f in parquet_files]
        else:
            files = [(f, "csv") for f in csv_files]

        if not files:
            print(f"[WARN] Nessun file (parquet/csv) nel batch: {batch_dir}")
            # Non marchiamo il dt: così non lo perdiamo
            continue

        # Processiamo tutti i file del batch
        for f, kind in files:
            table_name = f.stem.lower().replace("-", "_").replace(" ", "_")
            src_path = f.as_posix()

            if not _table_exists(con, "bronze", table_name):
                # Prima creazione: prendiamo tutte le colonne del file e aggiungiamo ingest_dt
                if kind == "parquet":
                    con.execute(f"""
                        CREATE TABLE bronze.{table_name} AS
                        SELECT *, '{dt}' AS ingest_dt
                        FROM read_parquet('{src_path}');
                    """)
                else:
                    con.execute(f"""
                        CREATE TABLE bronze.{table_name} AS
                        SELECT *, '{dt}' AS ingest_dt
                        FROM read_csv_auto('{src_path}', header=True);
                    """)
                print("[BRONZE CREATE]", table_name, "dt=", dt)
            else:
                # Insert allineato alle colonne target (robusto a schema drift)
                _insert_aligned(con, "bronze", table_name, kind, src_path, dt)
                print("[BRONZE INSERT]", table_name, "dt=", dt)

        # Commit logico del batch: SOLO se tutti i file del batch sono stati processati
        con.execute("INSERT INTO meta.processed_batches(dt) VALUES (?)", [dt])
        print("[LEDGER]", "marked processed dt=", dt)

    con.close()
    return to_process
