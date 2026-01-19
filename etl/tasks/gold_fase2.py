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
    GOLD Fase 2 (contract per Streamlit):
    - costruisce dim_* e fact_* partendo da silver.*
    - espone colonne in snake_case (race_id, driver_id, season_year, ...)
    - aggiunge alcune colonne "legacy-friendly" (year, name, date, time, url) per evitare rotture
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

    # dim_driver
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_driver AS
        SELECT
            driverId    AS driver_id,
            driverRef   AS driver_ref,
            number      AS driver_number,
            code        AS driver_code,
            forename    AS forename,
            surname     AS surname,
            dob         AS dob,
            nationality AS nationality
        FROM silver.drivers;
    """)
    built.append("dim_driver")
    _qc_not_null(con, "dim_driver", ["driver_id"])
    _qc_duplicates(con, "dim_driver", ["driver_id"])

    # dim_constructor
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_constructor AS
        SELECT
            constructorId   AS constructor_id,
            constructorRef  AS constructor_ref,
            name            AS constructor_name,
            nationality     AS nationality
        FROM silver.constructors;
    """)
    built.append("dim_constructor")
    _qc_not_null(con, "dim_constructor", ["constructor_id"])
    _qc_duplicates(con, "dim_constructor", ["constructor_id"])

    # dim_circuit
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_circuit AS
        SELECT
            circuitId   AS circuit_id,
            circuitRef  AS circuit_ref,
            name        AS circuit_name,
            location    AS location,
            country     AS country,
            lat         AS lat,
            lng         AS lng,
            alt         AS alt
        FROM silver.circuits;
    """)
    built.append("dim_circuit")
    _qc_not_null(con, "dim_circuit", ["circuit_id"])
    _qc_duplicates(con, "dim_circuit", ["circuit_id"])

    # dim_status
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_status AS
        SELECT
            statusId AS status_id,
            status   AS status
        FROM silver.status;
    """)
    built.append("dim_status")
    _qc_not_null(con, "dim_status", ["status_id"])
    _qc_duplicates(con, "dim_status", ["status_id"])

    # dim_race (compatibile Streamlit: season_year + race_id)
    # Aggiungo anche alias "legacy" (year, name, date, time, url) perché spesso vengono usati nelle query.
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_race AS
        SELECT
            raceId      AS race_id,
            year        AS season_year,
            year        AS year,
            round       AS round,
            circuitId   AS circuit_id,

            -- nomi "comodi"
            name        AS race_name,
            name        AS name,

            date        AS race_date,
            date        AS date,

            time        AS race_time,
            time        AS time,

            url         AS race_url,
            url         AS url
        FROM silver.races;
    """)
    built.append("dim_race")
    _qc_not_null(con, "dim_race", ["race_id"])
    _qc_duplicates(con, "dim_race", ["race_id"])

    # ----------------
    # FACT
    # ----------------
    # Grain: 1 riga = 1 driver in 1 race (risultato gara)
    con.execute("""
        CREATE OR REPLACE TABLE gold.fact_race_results AS
        SELECT
            resultId        AS result_id,
            raceId          AS race_id,
            driverId        AS driver_id,
            constructorId   AS constructor_id,

            number          AS car_number,
            grid            AS grid,
            position        AS position,
            positionText    AS position_text,
            positionOrder   AS position_order,
            points          AS points,
            laps            AS laps,

            -- time: naming "comodo"
            time            AS race_time,
            milliseconds    AS milliseconds,

            fastestLap      AS fastest_lap,
            rank            AS fastest_lap_rank,
            fastestLapTime  AS fastest_lap_time,
            fastestLapSpeed AS fastest_lap_speed,

            statusId        AS status_id
        FROM silver.results;
    """)
    built.append("fact_race_results")

    _qc_not_null(con, "fact_race_results", ["result_id", "race_id", "driver_id"])
    _qc_duplicates(con, "fact_race_results", ["result_id"])

    con.close()
    print("[GOLD OK]", built)
    return built
