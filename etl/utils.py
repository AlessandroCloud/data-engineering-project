from __future__ import annotations
import os
from pathlib import Path
import duckdb
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB = BASE_DIR / "data" / "warehouse.duckdb"

DB_PATH = os.getenv("DB_PATH", DEFAULT_DB.as_posix())


def get_connection(read_only: bool = True):
    """
    Connette a DuckDB in modo sicuro.
    In modalità container il DB è montato in /app/data/warehouse.duckdb
    """
    try:
        con = duckdb.connect(DB_PATH, read_only=read_only)
        return con
    except Exception as e:
        raise RuntimeError(f"[DuckDB] Errore apertura DB: {DB_PATH}\n{e}")
