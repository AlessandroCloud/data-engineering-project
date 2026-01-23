from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, date
import random

import polars as pl


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "f1"
LAKE_RAW = PROJECT_ROOT / "data_lake" / "raw"

F1_POINTS = {
    1: 25, 2: 18, 3: 15, 4: 12, 5: 10,
    6: 8, 7: 6, 8: 4, 9: 2, 10: 1
}


def _dt_value() -> str:
    return os.getenv("BATCH_DT") or datetime.now().strftime("%Y-%m-%d_%H%M%S")


def _read_csv(name: str) -> pl.DataFrame:
    p = RAW_DIR / f"{name}.csv"
    if not p.exists():
        raise FileNotFoundError(f"Missing source CSV: {p}")

    common_kwargs = dict(
        infer_schema_length=10000,
        null_values=["\\N"],
    )

    n = name.lower()

    if n == "results":
        df = pl.read_csv(
            p,
            **common_kwargs,
            schema_overrides={
                "points": pl.Float64,
                "milliseconds": pl.Int64,
                "rank": pl.Int64,
                "fastestLap": pl.Int64,
            },
        )
        return df.with_columns([
            pl.col("points").cast(pl.Float64, strict=False),
            pl.col("milliseconds").cast(pl.Int64, strict=False),
            pl.col("rank").cast(pl.Int64, strict=False),
            pl.col("fastestLap").cast(pl.Int64, strict=False),
        ])

    if n == "races":
        df = pl.read_csv(
            p,
            **common_kwargs,
            schema_overrides={
                "year": pl.Int64,
                "round": pl.Int64,
                "raceId": pl.Int64,
                "circuitId": pl.Int64,
            },
        )
        # date/time le lasciamo stringa: è raw, le normalizzi in Silver
        return df.with_columns([
            pl.col("year").cast(pl.Int64, strict=False),
            pl.col("round").cast(pl.Int64, strict=False),
        ])

    # default per gli altri CSV
    return pl.read_csv(p, **common_kwargs)





def main() -> None:
    dt = _dt_value()
    out_dir = LAKE_RAW / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)

    random.seed(dt)

    races = _read_csv("races")
    results = _read_csv("results")

    max_year = int(races.select(pl.col("year").max()).item())
    new_year = max_year + 1  # se max è 2024 -> 2025

    base_year_env = os.getenv("BASE_YEAR")
    base_year = int(base_year_env) if base_year_env else max_year

    base_races = (
        races
        .filter(pl.col("year") == base_year)
        .sort("round")
        .select(["raceId", "year", "round", "circuitId", "name", "date", "time", "url"])
    )
    if base_races.is_empty():
        raise ValueError(f"Nessuna gara trovata per base_year={base_year}")

    max_race_id = int(races.select(pl.col("raceId").max()).item())
    n_races = base_races.height
    new_race_ids = list(range(max_race_id + 1, max_race_id + 1 + n_races))

    start_date = date(new_year, 3, 1)

    new_races = (
        base_races
        .with_columns([
            pl.Series("raceId", new_race_ids),
            pl.lit(new_year).alias("year"),
            (pl.lit(start_date) + (pl.col("round") - 1) * pl.duration(days=7)).alias("date"),
            pl.when(pl.col("name").cast(pl.Utf8).str.contains(r"\(\d{4}\)"))
              .then(pl.col("name").cast(pl.Utf8).str.replace(r"\(\d{4}\)", f"({new_year})"))
              .otherwise(pl.col("name").cast(pl.Utf8) + f" ({new_year})")
              .alias("name"),
        ])
        .drop("time")
        .with_columns([pl.lit(None).alias("time")])
        .select(["raceId", "year", "round", "circuitId", "name", "date", "time", "url"])
    )

    base_race_ids = base_races.select("raceId").to_series().to_list()

    base_results_subset = (
        results
        .filter(pl.col("raceId").is_in(base_race_ids))
        .select(["driverId", "constructorId"])
        .unique()
    )

    entries = base_results_subset.to_dicts()
    if len(entries) < 20:
        raise ValueError("Non ci sono abbastanza driver/constructor entries per generare 20 risultati per gara.")
    entries = random.sample(entries, k=20)

    max_result_id = int(results.select(pl.col("resultId").max()).item())

    new_results_rows = []
    result_id_counter = max_result_id + 1

    for new_race_id in new_race_ids:
        shuffled = entries[:]
        random.shuffle(shuffled)

        for pos, entry in enumerate(shuffled, start=1):
            points = float(F1_POINTS.get(pos, 0))

            new_results_rows.append({
                "resultId": result_id_counter,
                "raceId": new_race_id,
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
                "statusId": 1,
            })
            result_id_counter += 1

    new_results = pl.DataFrame(new_results_rows)

    new_races.write_parquet(out_dir / "races.parquet")
    new_results.write_parquet(out_dir / "results.parquet")

    print(f"[OK] Synthetic season generated: year={new_year} from base_year={base_year} (dataset max_year={max_year})")
    print(f"[OK] Batch: {out_dir}")
    print(f"[OK] races rows={new_races.height}, results rows={new_results.height}")


if __name__ == "__main__":
    main()
