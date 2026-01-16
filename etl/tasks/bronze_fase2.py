from pathlib import Path
from prefect import task
from etl.utils import get_connection

def _ensure_metadata(con):
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
    dts = []
    for p in root.iterdir():
        if p.is_dir() and p.name.startswith("dt="):
            dts.append(p.name.replace("dt=", ""))
    return sorted(dts)

@task
def load_bronze_f1_incremental(raw_root: str = "data_lake/raw") -> list[str]:
    """
    Fase 2:
    - legge i batch raw organizzati in data_lake/raw/dt=YYYY-MM-DD/
    - processa solo i dt non ancora presenti in meta.processed_batches
    - carica in bronze.* con ingest_dt
    """
    con = get_connection(read_only=False)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    _ensure_metadata(con)

    all_dts = _list_raw_batches(raw_root)
    if not all_dts:
        con.close()
        return []

    processed = {
        r[0] for r in con.execute("SELECT dt FROM meta.processed_batches").fetchall()
    }
    to_process = [dt for dt in all_dts if dt not in processed]

    print("Found dt:", all_dts)
    print("Already processed dt:", sorted(processed))
    print("To process:", to_process)

    for dt in to_process:
        batch_dir = Path(raw_root) / f"dt={dt}"
        parquet_files = sorted(batch_dir.glob("*.parquet"))
        csv_files = sorted(batch_dir.glob("*.csv"))

        # Supporto: se nel batch hai parquet usiamo quelli, altrimenti csv
        files = parquet_files if parquet_files else csv_files
        if not files:
            print(f"[WARN] Nessun file trovato nel batch: {batch_dir}")
            continue

        # Carico ogni dataset
        for f in files:
            table_name = f.stem.lower().replace("-", "_").replace(" ", "_")

            if f.suffix.lower() == ".parquet":
                con.execute(f"""
                    INSERT INTO bronze.{table_name}
                    SELECT *, '{dt}' AS ingest_dt
                    FROM read_parquet('{f.as_posix()}');
                """) if _table_exists(con, "bronze", table_name) else con.execute(f"""
                    CREATE TABLE bronze.{table_name} AS
                    SELECT *, '{dt}' AS ingest_dt
                    FROM read_parquet('{f.as_posix()}');
                """)
            else:
                con.execute(f"""
                    INSERT INTO bronze.{table_name}
                    SELECT *, '{dt}' AS ingest_dt
                    FROM read_csv_auto('{f.as_posix()}', header=True);
                """) if _table_exists(con, "bronze", table_name) else con.execute(f"""
                    CREATE TABLE bronze.{table_name} AS
                    SELECT *, '{dt}' AS ingest_dt
                    FROM read_csv_auto('{f.as_posix()}', header=True);
                """)

        # Se tutto ok, marchio il batch come processato (commit logico)
        con.execute("INSERT INTO meta.processed_batches(dt) VALUES (?)", [dt])

    con.close()
    return to_process

def _table_exists(con, schema: str, table: str) -> bool:
    q = """
    SELECT COUNT(*) 
    FROM information_schema.tables 
    WHERE table_schema = ? AND table_name = ?
    """
    return con.execute(q, [schema, table]).fetchone()[0] > 0
