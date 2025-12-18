from __future__ import annotations

from prefect import task
from etl.utils import get_connection


# ---- SQL helpers ----
# Converte stringhe tipo "1:26.572" o "59.123" in millisecondi.
# - Se c'è ":", interpreta come M:SS.mmm
# - Se non c'è ":", interpreta come SS.mmm
# - Se NULL / '' / '\N' -> NULL
def sql_time_to_ms(col: str) -> str:
    return f"""
    CASE
      WHEN {col} IS NULL THEN NULL
      WHEN TRIM({col}) IN ('', '\\\\N') THEN NULL
      WHEN INSTR({col}, ':') > 0 THEN
        (
          TRY_CAST(SPLIT_PART({col}, ':', 1) AS INTEGER) * 60000
          +
          TRY_CAST(SPLIT_PART(SPLIT_PART({col}, ':', 2), '.', 1) AS INTEGER) * 1000
          +
          TRY_CAST(RPAD(SPLIT_PART({col}, '.', 2), 3, '0') AS INTEGER)
        )
      ELSE
        (
          TRY_CAST(SPLIT_PART({col}, '.', 1) AS INTEGER) * 1000
          +
          TRY_CAST(RPAD(SPLIT_PART({col}, '.', 2), 3, '0') AS INTEGER)
        )
    END
    """


@task(name="build_silver")
def build_silver() -> dict:
    """
    Crea il layer SILVER a partire dal BRONZE dentro DuckDB.
    Silver = pulizia tecnica: tipi coerenti, naming coerente, gestione valori sporchi.
    """
    con = get_connection()

    # Schema
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

    # -------------------------
    # DIMENSION-LIKE TABLES
    # -------------------------
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.drivers AS
        SELECT
            driverId::INTEGER                      AS driver_id,
            driverRef::VARCHAR                     AS driver_ref,
            forename::VARCHAR                      AS forename,
            surname::VARCHAR                       AS surname,
            TRY_CAST(NULLIF(dob, '') AS DATE)      AS dob,
            nationality::VARCHAR                   AS nationality,
            NULLIF(code, '')::VARCHAR              AS code,
            TRY_CAST(NULLIF(number, '') AS INTEGER) AS driver_number
        FROM bronze.drivers;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE silver.constructors AS
        SELECT
            constructorId::INTEGER     AS constructor_id,
            constructorRef::VARCHAR    AS constructor_ref,
            name::VARCHAR              AS constructor_name,
            nationality::VARCHAR       AS nationality
        FROM bronze.constructors;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE silver.circuits AS
        SELECT
            circuitId::INTEGER          AS circuit_id,
            circuitRef::VARCHAR         AS circuit_ref,
            name::VARCHAR               AS circuit_name,
            location::VARCHAR           AS location,
            country::VARCHAR            AS country,
            TRY_CAST(lat AS DOUBLE)     AS lat,
            TRY_CAST(lng AS DOUBLE)     AS lng,
            TRY_CAST(alt AS INTEGER)    AS alt
        FROM bronze.circuits;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE silver.races AS
        SELECT
            raceId::INTEGER                 AS race_id,
            year::INTEGER                   AS season_year,
            round::INTEGER                  AS round,
            name::VARCHAR                   AS race_name,
            TRY_CAST(NULLIF(date, '') AS DATE) AS race_date,
            circuitId::INTEGER              AS circuit_id
        FROM bronze.races;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE silver.status AS
        SELECT
            statusId::INTEGER AS status_id,
            status::VARCHAR   AS status
        FROM bronze.status;
        """
    )

    # -------------------------
    # FACT-LIKE TABLES
    # -------------------------

    # results (fact centrale futura)
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.results AS
        SELECT
            resultId::INTEGER                     AS result_id,
            raceId::INTEGER                       AS race_id,
            driverId::INTEGER                     AS driver_id,
            constructorId::INTEGER                AS constructor_id,
            statusId::INTEGER                     AS status_id,

            TRY_CAST(NULLIF(grid, '\\\\N') AS INTEGER)      AS grid,
            TRY_CAST(NULLIF(position, '\\\\N') AS INTEGER)  AS position,
            positionOrder::INTEGER                 AS position_order,
            TRY_CAST(points AS DOUBLE)             AS points,

            TRY_CAST(NULLIF(laps, '\\\\N') AS INTEGER)      AS laps,
            TRY_CAST(NULLIF(milliseconds, '\\\\N') AS BIGINT) AS milliseconds
        FROM bronze.results;
        """
    )

    # qualifying
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.qualifying AS
        SELECT
            qualifyId::INTEGER   AS qualify_id,
            raceId::INTEGER      AS race_id,
            driverId::INTEGER    AS driver_id,
            constructorId::INTEGER AS constructor_id,
            TRY_CAST(NULLIF(number, '') AS INTEGER) AS car_number,
            TRY_CAST(NULLIF(position, '\\\\N') AS INTEGER) AS qualifying_position,

            {sql_time_to_ms("q1")} AS q1_ms,
            {sql_time_to_ms("q2")} AS q2_ms,
            {sql_time_to_ms("q3")} AS q3_ms
        FROM bronze.qualifying;
        """
    )

    # pit_stops (durata in secondi spesso come stringa "26.898")
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.pit_stops AS
        SELECT
            raceId::INTEGER                         AS race_id,
            driverId::INTEGER                       AS driver_id,
            TRY_CAST(NULLIF(stop, '\\N') AS INTEGER) AS stop_number,
            TRY_CAST(NULLIF(lap, '\\N') AS INTEGER)  AS lap,
            time::VARCHAR                           AS time_of_day,

            -- duration è spesso in secondi con decimali -> ms
            CASE
              WHEN duration IS NULL THEN NULL
              WHEN TRIM(duration) IN ('', '\\N') THEN NULL
              ELSE TRY_CAST(duration AS DOUBLE) * 1000
            END AS pit_duration_ms
        FROM bronze.pit_stops;
        """
    )

    # lap_times (tempo giro come "M:SS.mmm")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.lap_times AS
        SELECT
            raceId::INTEGER                        AS race_id,
            driverId::INTEGER                      AS driver_id,
            TRY_CAST(NULLIF(lap, '\\\\N') AS INTEGER) AS lap_number,
            TRY_CAST(NULLIF(position, '\\\\N') AS INTEGER) AS lap_position,
            {sql_time_to_ms("time")}               AS lap_time_ms
        FROM bronze.lap_times;
        """
    )

    # driver_standings
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.driver_standings AS
        SELECT
            driverStandingsId::INTEGER AS driver_standings_id,
            raceId::INTEGER            AS race_id,
            driverId::INTEGER          AS driver_id,
            TRY_CAST(points AS DOUBLE) AS championship_points,
            TRY_CAST(position AS INTEGER) AS championship_position,
            positionText::VARCHAR      AS position_text,
            TRY_CAST(wins AS INTEGER)  AS wins_to_date
        FROM bronze.driver_standings;
        """
    )

    # constructor_standings
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.constructor_standings AS
        SELECT
            constructorStandingsId::INTEGER AS constructor_standings_id,
            raceId::INTEGER                 AS race_id,
            constructorId::INTEGER          AS constructor_id,
            TRY_CAST(points AS DOUBLE)      AS championship_points,
            TRY_CAST(position AS INTEGER)   AS championship_position,
            positionText::VARCHAR           AS position_text,
            TRY_CAST(wins AS INTEGER)       AS wins_to_date
        FROM bronze.constructor_standings;
        """
    )

    # -------------------------
    # QUALITY CHECKS (minimi)
    # -------------------------
    checks = [
        ("silver.drivers", "driver_id"),
        ("silver.constructors", "constructor_id"),
        ("silver.circuits", "circuit_id"),
        ("silver.races", "race_id"),
        ("silver.status", "status_id"),
        ("silver.results", "result_id"),
    ]
    for table, key in checks:
        nulls = con.execute(f"SELECT COUNT(*) FROM {table} WHERE {key} IS NULL").fetchone()[0]
        if nulls != 0:
            con.close()
            raise ValueError(f"[QUALITY FAIL] {table}.{key} ha {nulls} NULL")

    # piccolo report tabelle/righe
    summary = {}
    silver_tables = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'silver'
        ORDER BY table_name
        """
    ).fetchall()

    for (tname,) in silver_tables:
        cnt = con.execute(f"SELECT COUNT(*) FROM silver.{tname}").fetchone()[0]
        summary[f"silver.{tname}"] = int(cnt)

    con.close()
    return summary
