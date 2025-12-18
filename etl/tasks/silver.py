from __future__ import annotations

from prefect import task
from etl.utils import get_connection


# -------------------------
# SQL HELPERS (no '\N'!)
# -------------------------

def sql_time_to_ms(col: str) -> str:
    """
    Converte tempi tipo:
      - "1:26.572" -> millisecondi
      - "59.123"   -> millisecondi
      - NULL / ''  -> NULL

    Nota: NON controlliamo mai '\N' per evitare problemi di escape Python.
    Se arriva una stringa non parsabile, TRY_CAST/TRY_STRPTIME porteranno a NULL.
    """
    return f"""
    CASE
      WHEN {col} IS NULL THEN NULL
      WHEN NULLIF(TRIM(CAST({col} AS VARCHAR)), '') IS NULL THEN NULL
      WHEN INSTR(CAST({col} AS VARCHAR), ':') > 0 THEN
        (
          TRY_CAST(SPLIT_PART(CAST({col} AS VARCHAR), ':', 1) AS INTEGER) * 60000
          +
          TRY_CAST(SPLIT_PART(SPLIT_PART(CAST({col} AS VARCHAR), ':', 2), '.', 1) AS INTEGER) * 1000
          +
          TRY_CAST(RPAD(SPLIT_PART(CAST({col} AS VARCHAR), '.', 2), 3, '0') AS INTEGER)
        )
      ELSE
        (
          TRY_CAST(SPLIT_PART(CAST({col} AS VARCHAR), '.', 1) AS INTEGER) * 1000
          +
          TRY_CAST(RPAD(SPLIT_PART(CAST({col} AS VARCHAR), '.', 2), 3, '0') AS INTEGER)
        )
    END
    """


def sql_safe_date(col: str) -> str:
    """
    Parsing robusto date:
    - se già DATE -> ok
    - se NULL / '' -> NULL
    - altrimenti prova YYYY-MM-DD, se non valido -> NULL
    """
    return f"""
    CASE
      WHEN {col} IS NULL THEN NULL
      WHEN typeof({col}) = 'DATE' THEN {col}
      ELSE TRY_STRPTIME(NULLIF(TRIM(CAST({col} AS VARCHAR)), ''), '%Y-%m-%d')::DATE
    END
    """


def sql_clean_int(col: str) -> str:
    """Converte a INTEGER in modo safe: NULL / '' -> NULL, altrimenti TRY_CAST."""
    return f"TRY_CAST(NULLIF(TRIM(CAST({col} AS VARCHAR)), '') AS INTEGER)"


def sql_clean_bigint(col: str) -> str:
    """Converte a BIGINT in modo safe: NULL / '' -> NULL, altrimenti TRY_CAST."""
    return f"TRY_CAST(NULLIF(TRIM(CAST({col} AS VARCHAR)), '') AS BIGINT)"


def sql_clean_double(col: str) -> str:
    """Converte a DOUBLE in modo safe: NULL / '' -> NULL, altrimenti TRY_CAST."""
    return f"TRY_CAST(NULLIF(TRIM(CAST({col} AS VARCHAR)), '') AS DOUBLE)"


@task(name="build_silver")
def build_silver() -> dict:
    """
    SILVER = pulizia tecnica del BRONZE:
    - tipi coerenti
    - naming coerente
    - gestione valori sporchi
    - NO KPI / NO aggregazioni / NO business logic
    """
    con = get_connection()
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

    # -------------------------
    # DIMENSION-LIKE TABLES
    # -------------------------

    # drivers
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.drivers AS
        SELECT
            driverId::INTEGER                            AS driver_id,
            driverRef::VARCHAR                           AS driver_ref,
            forename::VARCHAR                            AS forename,
            surname::VARCHAR                             AS surname,
            {sql_safe_date("dob")}                       AS dob,
            nationality::VARCHAR                         AS nationality,
            NULLIF(TRIM(CAST(code AS VARCHAR)), '')      AS code,
            {sql_clean_int("number")}                    AS driver_number
        FROM bronze.drivers;
        """
    )

    # constructors
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

    # circuits
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

    # races
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.races AS
        SELECT
            raceId::INTEGER            AS race_id,
            year::INTEGER              AS season_year,
            round::INTEGER             AS round,
            name::VARCHAR              AS race_name,
            {sql_safe_date("date")}    AS race_date,
            circuitId::INTEGER         AS circuit_id
        FROM bronze.races;
        """
    )

    # status
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

    # results
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.results AS
        SELECT
            resultId::INTEGER                  AS result_id,
            raceId::INTEGER                    AS race_id,
            driverId::INTEGER                  AS driver_id,
            constructorId::INTEGER             AS constructor_id,
            statusId::INTEGER                  AS status_id,

            {sql_clean_int("grid")}            AS grid,
            {sql_clean_int("position")}        AS position,
            positionOrder::INTEGER             AS position_order,
            TRY_CAST(points AS DOUBLE)         AS points,

            {sql_clean_int("laps")}            AS laps,
            {sql_clean_bigint("milliseconds")} AS milliseconds
        FROM bronze.results;
        """
    )

    # qualifying
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.qualifying AS
        SELECT
            qualifyId::INTEGER          AS qualify_id,
            raceId::INTEGER             AS race_id,
            driverId::INTEGER           AS driver_id,
            constructorId::INTEGER      AS constructor_id,
            {sql_clean_int("number")}   AS car_number,
            {sql_clean_int("position")} AS qualifying_position,

            {sql_time_to_ms("q1")}      AS q1_ms,
            {sql_time_to_ms("q2")}      AS q2_ms,
            {sql_time_to_ms("q3")}      AS q3_ms
        FROM bronze.qualifying;
        """
    )

    # pit_stops
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.pit_stops AS
        SELECT
            raceId::INTEGER                 AS race_id,
            driverId::INTEGER               AS driver_id,
            {sql_clean_int("stop")}         AS stop_number,
            {sql_clean_int("lap")}          AS lap,
            CAST(time AS VARCHAR)           AS time_of_day,

            -- duration spesso è in secondi (stringa/numero) -> ms
            CASE
              WHEN duration IS NULL THEN NULL
              WHEN NULLIF(TRIM(CAST(duration AS VARCHAR)), '') IS NULL THEN NULL
              ELSE {sql_clean_double("duration")} * 1000
            END AS pit_duration_ms
        FROM bronze.pit_stops;
        """
    )

    # lap_times
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.lap_times AS
        SELECT
            raceId::INTEGER               AS race_id,
            driverId::INTEGER             AS driver_id,
            {sql_clean_int("lap")}        AS lap_number,
            {sql_clean_int("position")}   AS lap_position,
            {sql_time_to_ms("time")}      AS lap_time_ms
        FROM bronze.lap_times;
        """
    )

    # driver_standings
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.driver_standings AS
        SELECT
            driverStandingsId::INTEGER          AS driver_standings_id,
            raceId::INTEGER                     AS race_id,
            driverId::INTEGER                   AS driver_id,
            {sql_clean_double("points")}        AS championship_points,
            {sql_clean_int("position")}         AS championship_position,
            positionText::VARCHAR               AS position_text,
            {sql_clean_int("wins")}             AS wins_to_date
        FROM bronze.driver_standings;
        """
    )

    # constructor_standings
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.constructor_standings AS
        SELECT
            constructorStandingsId::INTEGER     AS constructor_standings_id,
            raceId::INTEGER                     AS race_id,
            constructorId::INTEGER              AS constructor_id,
            {sql_clean_double("points")}        AS championship_points,
            {sql_clean_int("position")}         AS championship_position,
            positionText::VARCHAR               AS position_text,
            {sql_clean_int("wins")}             AS wins_to_date
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

    # -------------------------
    # SUMMARY
    # -------------------------
    summary: dict[str, int] = {}
    tables = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'silver'
        ORDER BY table_name
        """
    ).fetchall()

    for (tname,) in tables:
        cnt = con.execute(f"SELECT COUNT(*) FROM silver.{tname}").fetchone()[0]
        summary[f"silver.{tname}"] = int(cnt)

    con.close()
    return summary
