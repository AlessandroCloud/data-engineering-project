from __future__ import annotations

from prefect import task
from etl.utils import get_connection


@task(name="build_gold")
def build_gold() -> dict:
    """
    GOLD = modello analitico (star schema) a partire dal SILVER.
    Versione A (minimale ma completa):
      - dim_driver
      - dim_constructor
      - dim_circuit
      - dim_race
      - dim_status
      - fact_race_results
    """
    con = get_connection()
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    # -------------------------
    # DIMENSIONS
    # -------------------------

    con.execute(
        """
        CREATE OR REPLACE TABLE gold.dim_driver AS
        SELECT
            driver_id,
            driver_ref,
            forename,
            surname,
            dob,
            nationality,
            code,
            driver_number
        FROM silver.drivers;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE gold.dim_constructor AS
        SELECT
            constructor_id,
            constructor_ref,
            constructor_name,
            nationality
        FROM silver.constructors;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE gold.dim_circuit AS
        SELECT
            circuit_id,
            circuit_ref,
            circuit_name,
            location,
            country,
            lat,
            lng,
            alt
        FROM silver.circuits;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE gold.dim_race AS
        SELECT
            race_id,
            season_year,
            round,
            race_name,
            race_date,
            circuit_id
        FROM silver.races;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE gold.dim_status AS
        SELECT
            status_id,
            status
        FROM silver.status;
        """
    )

    # -------------------------
    # FACT (grain = 1 riga per driver per race)
    # -------------------------
    con.execute(
        """
        CREATE OR REPLACE TABLE gold.fact_race_results AS
        SELECT
            r.result_id,
            r.race_id,
            ra.season_year,
            ra.round,
            ra.race_date,
            ra.circuit_id,

            r.driver_id,
            r.constructor_id,
            r.status_id,

            r.grid,
            r.position,
            r.position_order,
            r.points,
            r.laps,
            r.milliseconds
        FROM silver.results r
        LEFT JOIN silver.races ra
          ON ra.race_id = r.race_id;
        """
    )

    # -------------------------
    # QUALITY CHECKS (minimi)
    # -------------------------
    # 1) Fact deve avere chiavi non NULL
    qc_nulls = con.execute(
        """
        SELECT
          SUM(CASE WHEN result_id IS NULL THEN 1 ELSE 0 END) AS null_result_id,
          SUM(CASE WHEN race_id IS NULL THEN 1 ELSE 0 END)   AS null_race_id,
          SUM(CASE WHEN driver_id IS NULL THEN 1 ELSE 0 END) AS null_driver_id
        FROM gold.fact_race_results;
        """
    ).fetchone()

    if qc_nulls[0] != 0 or qc_nulls[1] != 0 or qc_nulls[2] != 0:
        con.close()
        raise ValueError(f"[QUALITY FAIL] NULL keys in fact_race_results: {qc_nulls}")

    # 2) Count fact = count silver.results (idealmente)
    silver_cnt = con.execute("SELECT COUNT(*) FROM silver.results;").fetchone()[0]
    gold_cnt = con.execute("SELECT COUNT(*) FROM gold.fact_race_results;").fetchone()[0]
    if silver_cnt != gold_cnt:
        con.close()
        raise ValueError(
            f"[QUALITY FAIL] Conteggio mismatch: silver.results={silver_cnt} vs gold.fact_race_results={gold_cnt}"
        )

    # -------------------------
    # SUMMARY
    # -------------------------
    summary: dict[str, int] = {}
    tables = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'gold'
        ORDER BY table_name
        """
    ).fetchall()

    for (tname,) in tables:
        cnt = con.execute(f"SELECT COUNT(*) FROM gold.{tname}").fetchone()[0]
        summary[f"gold.{tname}"] = int(cnt)

    con.close()
    return summary
