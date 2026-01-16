import os
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", "data/warehouse.duckdb"))
DATA_LAKE_PATH = Path(os.getenv("DATA_LAKE_PATH", "data_lake"))

RAW_PATH = DATA_LAKE_PATH / "raw"
BRONZE_PATH = DATA_LAKE_PATH / "bronze"
SILVER_PATH = DATA_LAKE_PATH / "silver"
GOLD_PATH = DATA_LAKE_PATH / "gold"
META_PATH = DATA_LAKE_PATH / "_metadata"
