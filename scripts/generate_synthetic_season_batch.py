from __future__ import annotations

import os
import random
from datetime import datetime, date, timedelta
from pathlib import Path

import duckdb
import polars as pl


# -------------------------
# Paths
# -------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "warehouse.duckdb"
LAKE_RAW = PROJECT_ROOT / "data_lake" / "raw"

# Points system (classic modern top10)
F1_POINTS = {
    1: 25, 2: 18, 3: 15, 4: 12, 5: 10,
    6: 8, 7: 6, 8: 4, 9: 2, 10: 1
}


# -------------------------
# Helpers
# -------------------------
def _dt_value() -> str:
    """
    Partition dt for Data Lake: YYYY-MM-DD (daily ledger).
    - If BATCH_DT passed (e.g. in Actions), use only the date part.
    - Else use today's date.
    """
    v = os.getenv("BATCH_DT")
    if v:
        return v[:10]
    return datetime.now().strftime("%Y-%m-%d")


def _connect_ro() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Warehouse DuckDB non trovato: {DB_PATH}. "
            "Assicurati che data/warehouse.duckdb sia presente nel repo o generato dalla pipeline."
        )
    return duckdb.connect(DB_PATH.as_posix(), read_only=True)


def _gold_exists(con: duckdb.DuckDBPyConnection) -> None:
    # basic sanity: ensure gold schema/tables exist
    tables = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='gold'
    """).fetchall()
    if not tables:
        raise RuntimeError(
            "Schema GOLD non trovato nel warehouse. "
            "Esegui prima la pipeline (bronze→silver→gold) o committa il warehouse aggiornato."
        )


def _pick_base_year(con: duckdb.DuckDBPyConnection) -> int:
    """
    Base season to clone structure from.
    - If BASE_YEAR env provided, use it.
    - Else: use max available year in gold.dim_race.
    """
    base_env = os.getenv("BASE_YEAR")
    if base_env:
        return int(base_env)

    max_year = con.execute("SELECT MAX(year) FROM gold.dim_race").fetchone()[0]
    if max_year is None:
        raise RuntimeError("Impossibile determinare l'anno massimo da gold.dim_race.")
    return int(max_year)


def _next_year(con: duckdb.DuckDBPyConnection) -> int:
    """
    New synthetic season year:
    - It must be >= 2025
    - And not collide with existing years already in GOLD.
    """
    max_year = con.execute("SELECT MAX(year) FROM gold.dim_race").fetchone()[0]
    max_year = int(max_year) if max_year is not None else 2024
    candidate = max_year + 1
    return max(candidate, 2025)


def _make_race_dates(new_year: int, rounds: list[int]) -> list[str]:
    """
    Generate simple weekly dates starting from March 1st (YYYY-MM-DD).
    Keep it deterministic and valid (string).
    """
    start = date(new_year, 3, 1)
    out = []
    for r in rounds:
        d = start + timedelta(days=(r - 1) * 7)
        out.append(d.isoformat())
    return out


# -------------------------
# Main
# -------------------------
def main() -> None:
    dt = _dt_value()  # partition folder dt=YYYY-MM-DD
    out_dir = LAKE_RAW / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # deterministic seed per day (same day => same season results)
    random.seed(dt)

    con = _connect_ro()
    _gold_exists(con)

    base_year = _pick_base_year(con)
    new_year = _next_year(con)

    # Read base races from GOLD (schema in gold.dim_race is: raceId, year, round, circuitId, name, date, time, url)
    # We clone the season structure (same rounds/circuits) and just update ids + year + dates.
    base_races = con.execute(f"""
        SELECT
            raceId,
            year,
            round,
            circuitId,
            name,
            date,
            time,
            url
        FROM gold.dim_race
        WHERE year = {base_year}
        ORDER BY round
    """).pl()

    if base_races.is_empty():
        raise ValueError(f"Nessuna gara trovata in gold.dim_race per base_year={base_year}")

    # Determine new race IDs
    max_race_id = con.execute("SELECT MAX(raceId) FROM gold.dim_race").fetchone()[0]
    max_race_id = int(max_race_id) if max_race_id is not None else 0

    rounds = base_races["round"].to_list()
    n_races = len(rounds)
    new_race_ids = list(range(max_race_id + 1, max_race_id + 1 + n_races))

    # Build new races (keep columns compatible with your bronze ingestion expectations)
    new_dates = _make_race_dates(new_year, rounds)

    # Normalize name: append (YYYY) if missing, else replace existing (YYYY)
    # Keep `time` nullable.
    new_races = (
        base_races
        .with_columns([
            pl.Series("raceId", new_race_ids),
            pl.lit(new_year).alias("year"),
            pl.Series("date", new_dates).alias("date"),
            pl.when(pl.col("name").cast(pl.Utf8).str.contains(r"\(\d{4}\)"))
              .then(pl.col("name").cast(pl.Utf8).str.replace(r"\(\d{4}\)", f"({new_year})"))
              .otherwise(pl.col("name").cast(pl.Utf8) + f" ({new_year})")
              .alias("name"),
        ])
        .with_columns([pl.lit(None).alias("time")])
        .select(["raceId", "year", "round", "circuitId", "name", "date", "time", "url"])
    )

    # Build "entries" from base_year results: pick 20 unique (driverId, constructorId)
    base_race_ids = base_races["raceId"].to_list()

    base_entries = con.execute(f"""
        SELECT DISTINCT
            driverId,
            constructorId
        FROM gold.fact_race_results
        WHERE raceId IN ({",".join(map(str, base_race_ids))})
    """).pl()

    entries = base_entries.to_dicts()
    if len(entries) < 20:
        raise ValueError(
            f"Non ci sono abbastanza entry driver/constructor ({len(entries)}) "
            f"per generare 20 risultati per gara (base_year={base_year})."
        )
    entries = random.sample(entries, k=20)

    # Determine new result IDs
    max_result_id = con.execute("SELECT MAX(resultId) FROM gold.fact_race_results").fetchone()[0]
    max_result_id = int(max_result_id) if max_result_id is not None else 0
    result_id_counter = max_result_id + 1

    # Generate new results (20 rows per race)
    new_results_rows: list[dict] = []
    for race_id in new_race_ids:
        shuffled = entries[:]
        random.shuffle(shuffled)

        for pos, entry in enumerate(shuffled, start=1):
            points = float(F1_POINTS.get(pos, 0))

            new_results_rows.append({
                "resultId": result_id_counter,
                "raceId": int(race_id),
                "driverId": int(entry["driverId"]),
                "constructorId": int(entry["constructorId"]),
                "number": None,
                "grid": random.randint(1, 20),
                "position": pos,
                "positionText": str(pos),
                "positionOrder": pos,
                "points": points,
                "laps": random.randint(40, 70),
                "time": None,
                "milliseconds": None,
                "fastestLap": None,
                "rank": None,
                "fastestLapTime": None,
                "fastestLapSpeed": None,
                # statusId 1 usually "Finished"
                "statusId": 1,
            })
            result_id_counter += 1

    con.close()

    new_results = pl.DataFrame(new_results_rows)

    # Write batch parquet files
    new_races.write_parquet(out_dir / "races.parquet")
    new_results.write_parquet(out_dir / "results.parquet")

    print(f"[OK] Synthetic season generated: new_year={new_year} cloned_from={base_year}")
    print(f"[OK] Data Lake batch: {out_dir}")
    print(f"[OK] races rows={new_races.height}, results rows={new_results.height}")


if __name__ == "__main__":
    main()
