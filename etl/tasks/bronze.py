from pathlib import Path
from prefect import task
from etl.utils import get_connection

@task
def load_bronze_f1(raw_folder: str = "data/raw/f1") -> int:
    raw_path = Path(raw_folder)

    if not raw_path.exists():
        raise FileNotFoundError(f"Cartella RAW non trovata: {raw_folder}")

    csv_files = sorted(raw_path.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"Nessun CSV trovato in: {raw_folder}")

    con = get_connection()
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

    loaded = 0
    for csv_file in csv_files:
        table_name = csv_file.stem.lower().replace("-", "_").replace(" ", "_")

        
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.{table_name} AS
            SELECT * FROM read_csv_auto('{csv_file.as_posix()}', header=True);
        """)
        loaded += 1

    con.close()
    return loaded
