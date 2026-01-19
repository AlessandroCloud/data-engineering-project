from __future__ import annotations

from prefect import task
from etl.utils import get_connection


def _ensure_schema(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")


def _table_exists(con, schema: str, table: str) -> bool:
    q = """
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = ? AND table_name = ?
    """
    return con.execute(q, [schema, table]).fetchone()[0] > 0


def _require_silver(con, table: str) -> None:
    if not _table_exists(con, "silver", table):
        raise RuntimeError(f"[GOLD] Mancante: silver.{table}. Hai runnato silver_fase2?")


def _qc_not_null(con, table: str, cols: list[str]) -> None:
    cond = " OR ".join([f'"{c}" IS NULL' for c in cols])
    n = con.execute(f"SELECT COUNT(*) FROM gold.{table} WHERE {cond}").fetchone()[0]
    if n != 0:
        raise RuntimeError(f"[GOLD][QC] gold.{table} ha {n} righe con NULL su {cols}.")


def _qc_duplicates(con, table: str, key_cols: list[str]) -> None:
    group_by = ", ".join([f'"{c}"' for c in key_cols])
    n = con.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT {group_by}, COUNT(*) AS c
            FROM gold.{table}
            GROUP BY {group_by}
            HAVING COUNT(*) > 1
        ) t
    """).fetchone()[0]
    if n != 0:
        raise RuntimeError(f"[GOLD][QC] gold.{table} ha duplicati sulla chiave {key_cols} (gruppi duplicati: {n}).")


@task
def build_gold_f1() -> list[str]:
    """
    GOLD Fase 2:
    - costruisce dim_* e fact_race_results a partire da silver.*
    - mantiene nomi compatibili con la dashboard Fase 1 (se li usavi così)
    - quality checks base
    """
    con = get_connection(read_only=False)
    _ensure_schema(con)

    # requisiti minimi
    for t in ["drivers", "constructors", "circuits", "races", "status", "results"]:
        _require_silver(con, t)

    built: list[str] = []

    # ----------------
    # DIMENSIONS
    # ----------------

    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_driver AS
        SELECT
            driverId,
            driverRef,
            number,
            code,
            forename,
            surname,
            dob,
            nationality
        FROM silver.drivers;
    """)
    built.append("dim_driver")
    _qc_not_null(con, "dim_driver", ["driverId"])
    _qc_duplicates(con, "dim_driver", ["driverId"])

    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_constructor AS
        SELECT
            constructorId,
            constructorRef,
            name,
            nationality
        FROM silver.constructors;
    """)
    built.append("dim_constructor")
    _qc_not_null(con, "dim_constructor", ["constructorId"])
    _qc_duplicates(con, "dim_constructor", ["constructorId"])

    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_circuit AS
        SELECT
            circuitId,
            circuitRef,
            name,
            location,
            country,
            lat,
            lng,
            alt
        FROM silver.circuits;
    """)
    built.append("dim_circuit")
    _qc_not_null(con, "dim_circuit", ["circuitId"])
    _qc_duplicates(con, "dim_circuit", ["circuitId"])

    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_race AS
        SELECT
            raceId,
            year,
            round,
            circuitId,
            name,
            date,
            time,
            url
        FROM silver.races;
    """)
    built.append("dim_race")
    _qc_not_null(con, "dim_race", ["raceId"])
    _qc_duplicates(con, "dim_race", ["raceId"])

    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_status AS
        SELECT
            statusId,
            status
        FROM silver.status;
    """)
    built.append("dim_status")
    _qc_not_null(con, "dim_status", ["statusId"])
    _qc_duplicates(con, "dim_status", ["statusId"])

    # ----------------
    # FACT
    # ----------------
    # Grain: 1 riga = 1 driver in 1 race (risultato gara)
    # Nota: results ha sia driverId sia constructorId, più info di posizione/punti, ecc.
    con.execute("""
        CREATE OR REPLACE TABLE gold.fact_race_results AS
        SELECT
            r.resultId,
            r.raceId,
            r.driverId,
            r.constructorId,
            r.number,
            r.grid,
            r.position,
            r.positionText,
            r.positionOrder,
            r.points,
            r.laps,
            r.time,
            r.milliseconds,
            r.fastestLap,
            r.rank,
            r.fastestLapTime,
            r.fastestLapSpeed,
            r.statusId
        FROM silver.results r;
    """)
    built.append("fact_race_results")

    # QC fact: chiavi non null + unicità su resultId (se esiste)
    _qc_not_null(con, "fact_race_results", ["resultId", "raceId", "driverId"])
    _qc_duplicates(con, "fact_race_results", ["resultId"])

    con.close()
    print("[GOLD OK]", built)
    return built
