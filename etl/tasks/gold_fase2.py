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
    GOLD Fase 2 - Contract "super compatibile" per dashboard:
    - Espone colonne sia snake_case (con _) sia camelCase (senza _)
    - Aggiunge season_year anche nella fact (e seasonYear)
    - Consente query in dashboard anche se mischia naming conventions
    """
    con = get_connection(read_only=False)
    _ensure_schema(con)

    for t in ["drivers", "constructors", "circuits", "races", "status", "results"]:
        _require_silver(con, t)

    built: list[str] = []

    # ----------------
    # DIM DRIVER
    # ----------------
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_driver AS
        SELECT
            driverId    AS driver_id,
            driverId    AS driverId,

            driverRef   AS driver_ref,
            driverRef   AS driverRef,

            number      AS driver_number,
            number      AS driverNumber,

            code        AS driver_code,
            code        AS driverCode,

            forename    AS forename,
            surname     AS surname,
            dob         AS dob,
            nationality AS nationality
        FROM silver.drivers;
    """)
    built.append("dim_driver")
    _qc_not_null(con, "dim_driver", ["driver_id"])
    _qc_duplicates(con, "dim_driver", ["driver_id"])

    # ----------------
    # DIM CONSTRUCTOR
    # ----------------
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_constructor AS
        SELECT
            constructorId   AS constructor_id,
            constructorId   AS constructorId,

            constructorRef  AS constructor_ref,
            constructorRef  AS constructorRef,

            name            AS constructor_name,
            name            AS constructorName,
            name            AS name,

            nationality     AS nationality
        FROM silver.constructors;
    """)
    built.append("dim_constructor")
    _qc_not_null(con, "dim_constructor", ["constructor_id"])
    _qc_duplicates(con, "dim_constructor", ["constructor_id"])

    # ----------------
    # DIM CIRCUIT
    # ----------------
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_circuit AS
        SELECT
            circuitId   AS circuit_id,
            circuitId   AS circuitId,

            circuitRef  AS circuit_ref,
            circuitRef  AS circuitRef,

            name        AS circuit_name,
            name        AS circuitName,
            name        AS name,

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

    # ----------------
    # DIM STATUS
    # ----------------
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_status AS
        SELECT
            statusId AS status_id,
            statusId AS statusId,
            status   AS status
        FROM silver.status;
    """)
    built.append("dim_status")
    _qc_not_null(con, "dim_status", ["status_id"])
    _qc_duplicates(con, "dim_status", ["status_id"])

    # ----------------
    # DIM RACE
    # ----------------
    # Espongo:
    # - race_id + raceId
    # - season_year + seasonYear
    # - anche year, name, date, time, url (legacy)
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_race AS
        SELECT
            raceId      AS race_id,
            raceId      AS raceId,

            year        AS season_year,
            year        AS seasonYear,
            year        AS year,

            round       AS round,

            circuitId   AS circuit_id,
            circuitId   AS circuitId,

            name        AS race_name,
            name        AS raceName,
            name        AS name,

            date        AS race_date,
            date        AS raceDate,
            date        AS date,

            time        AS race_time,
            time        AS raceTime,
            time        AS time,

            url         AS race_url,
            url         AS raceUrl,
            url         AS url
        FROM silver.races;
    """)
    built.append("dim_race")
    _qc_not_null(con, "dim_race", ["race_id"])
    _qc_duplicates(con, "dim_race", ["race_id"])

    # ----------------
    # FACT RACE RESULTS
    # ----------------
    # Aggiungo season_year/seasonYear direttamente nella fact tramite join a races
    con.execute("""
        CREATE OR REPLACE TABLE gold.fact_race_results AS
        SELECT
            r.resultId        AS result_id,
            r.resultId        AS resultId,

            r.raceId          AS race_id,
            r.raceId          AS raceId,

            ra.year           AS season_year,
            ra.year           AS seasonYear,
            ra.year           AS year,

            r.driverId        AS driver_id,
            r.driverId        AS driverId,

            r.constructorId   AS constructor_id,
            r.constructorId   AS constructorId,

            r.number          AS car_number,
            r.number          AS carNumber,

            r.grid            AS grid,

            r.position        AS position,
            r.positionText    AS position_text,
            r.positionText    AS positionText,
            r.positionOrder   AS position_order,
            r.positionOrder   AS positionOrder,

            r.points          AS points,
            r.laps            AS laps,

            r.time            AS race_time,
            r.time            AS raceTime,
            r.milliseconds    AS milliseconds,

            r.fastestLap      AS fastest_lap,
            r.fastestLap      AS fastestLap,
            r.rank            AS fastest_lap_rank,
            r.rank            AS fastestLapRank,
            r.fastestLapTime  AS fastest_lap_time,
            r.fastestLapTime  AS fastestLapTime,
            r.fastestLapSpeed AS fastest_lap_speed,
            r.fastestLapSpeed AS fastestLapSpeed,

            r.statusId        AS status_id,
            r.statusId        AS statusId
        FROM silver.results r
        LEFT JOIN silver.races ra
            ON ra.raceId = r.raceId;
    """)
    built.append("fact_race_results")

    _qc_not_null(con, "fact_race_results", ["result_id", "race_id", "driver_id", "season_year"])
    _qc_duplicates(con, "fact_race_results", ["result_id"])

    con.close()
    print("[GOLD OK]", built)
    return built
