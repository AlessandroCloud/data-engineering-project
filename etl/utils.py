import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "data/warehouse.duckdb")

def get_connection():
    return duckdb.connect(DB_PATH)
