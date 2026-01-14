from __future__ import annotations
from pathlib import Path
import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]

DB_PATH = BASE_DIR / "data" / "warehouse.duckdb"

def get_connection(read_only: bool = True):
    """
    Apre una connessione a DuckDB.
    Nella dashboard usiamo sempre read_only=True.
    """
    try:
        con = duckdb.connect(DB_PATH.as_posix(), read_only=read_only)
        return con
    except Exception as e:
        raise RuntimeError(f"[DuckDB] Errore apertura DB: {DB_PATH}\n{e}")
