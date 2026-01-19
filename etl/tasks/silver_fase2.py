from __future__ import annotations

from prefect import task
from etl.utils import get_connection


# Chiavi naturali per dedup (silver = qualità)
# Nota: results nel dataset F1 tipicamente ha resultId, che è la chiave migliore.
SILVER_KEYS = {
    "circuits": ["circuitId"],
    "constructors": ["constructorId"],
    "drivers": ["driverId"],
    "races": ["raceId"],
    "status": ["statusId"],
    "results": ["resultId"],  # fallback possibile: ["raceId", "driverId"]
}


def _ensure_silver_schema(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")


def _table_exists(con, schema: str, table: str) -> bool:
    q = """
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = ? AND table_name = ?
    """
    return con.execute(q, [schema, table]).fetchone()[0] > 0


def _ensure_bronze_exists(con, table: str) -> None:
    if not _table_exists(con, "bronze", table):
        raise RuntimeError(
            f"[SILVER] Tabella bronze.{table} non trovata. "
            f"Hai runnato bronze_fase2?"
        )


def _column_exists(con, schema: str, table: str, col: str) -> bool:
    q = """
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = ? AND table_name = ? AND column_name = ?
    """
    return con.execute(q, [schema, table, col]).fetchone()[0] > 0


def _build_silver_table(con, table: str, keys: list[str]) -> None:
    """
    Rebuild deterministico:
    - prende tutto bronze.table
    - droppa righe con key null
    - dedup per chiave naturale
    - tiene la versione più recente usando ingest_dt (se presente)
    """
    _ensure_bronze_exists(con, table)

    # Verifica che le colonne chiave esistano davvero in bronze (evita query che esplodono)
    missing_keys = [k for k in keys if not _column_exists(con, "bronze", table, k)]
    if missing_keys:
        raise RuntimeError(
            f"[SILVER] In bronze.{table} mancano colonne chiave {missing_keys}. "
            f"Controlla SILVER_KEYS o lo schema bronze."
        )

    has_ingest_dt = _column_exists(con, "bronze", table, "ingest_dt")

    # Dedup con window function: rn=1 è la riga "migliore" per ogni chiave.
    # Se ingest_dt esiste: teniamo la più recente; altrimenti prendiamo una qualsiasi (stabile).
    partition_by = ", ".join([f'"{k}"' for k in keys])

    order_by = "ingest_dt DESC" if has_ingest_dt else partition_by

    # Condizione per drop null keys
    not_null_cond = " AND ".join([f'"{k}" IS NOT NULL' for k in keys])

    con.execute(f"""
        CREATE OR REPLACE TABLE silver.{table} AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_by}
                    ORDER BY {order_by}
                ) AS rn
            FROM bronze.{table}
            WHERE {not_null_cond}
        )
        SELECT * EXCLUDE (rn)
        FROM ranked
        WHERE rn = 1;
    """)


def _quality_checks(con, table: str, keys: list[str]) -> None:
    """
    Quality checks minimi ma utili:
    - righe totali > 0
    - nessun null sulle chiavi
    - nessun duplicato sulle chiavi
    """
    total = con.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()[0]
    if total == 0:
        raise RuntimeError(f"[SILVER][QC] silver.{table} è vuota.")

    # null sulle chiavi
    null_cond = " OR ".join([f'"{k}" IS NULL' for k in keys])
    nulls = con.execute(f"SELECT COUNT(*) FROM silver.{table} WHERE {null_cond}").fetchone()[0]
    if nulls != 0:
        raise RuntimeError(f"[SILVER][QC] silver.{table} ha {nulls} righe con chiavi NULL.")

    # duplicati sulle chiavi
    group_by = ", ".join([f'"{k}"' for k in keys])
    dup = con.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT {group_by}, COUNT(*) AS c
            FROM silver.{table}
            GROUP BY {group_by}
            HAVING COUNT(*) > 1
        ) t
    """).fetchone()[0]
    if dup != 0:
        raise RuntimeError(f"[SILVER][QC] silver.{table} ha duplicati sulle chiavi (gruppi duplicati: {dup}).")


@task
def build_silver_f1() -> list[str]:
    """
    Silver Fase 2:
    - rebuild deterministico da bronze
    - dedup per chiavi naturali
    - quality checks
    """
    con = get_connection(read_only=False)
    _ensure_silver_schema(con)

    built: list[str] = []

    for table, keys in SILVER_KEYS.items():
        _build_silver_table(con, table, keys)
        _quality_checks(con, table, keys)
        built.append(table)
        print("[SILVER OK]", table, "keys=", keys)

    con.close()
    return built
