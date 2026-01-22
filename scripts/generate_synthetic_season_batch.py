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
    """
    Ogni run crea un batch diverso.
    In CI conviene passare BATCH_DT, altrimenti usiamo timestamp locale.
    """
    return os.getenv("BATCH_DT") or datetime.now().strftime("%Y-%m-%d_%H%M%S")


def _read_csv(name: str) -> pl.DataFrame:
    p = RAW_DIR / f"{name}.csv"
    if not p.exists():
        raise FileNotFoundError(f"Missing source CSV: {p}")

    if name == "results":
        return pl.read_csv(
            p,
            try_parse_dates=True,
            infer_schema_length=10000,
            schema_overrides={
                "points": pl.Float64,
                # opzionali ma utili per stabilità:
                "milliseconds": pl.Int64,
                "rank": pl.Int64,
            },
        )

    return pl.read_csv(p, try_parse_dates=True, infer_schema_length=10000)

def main() -> None:
    dt = _dt_value()
    out_dir = LAKE_RAW / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # seed deterministico per run (replicabile)
    random.seed(dt)

    races = _read_csv("races")
    results = _read_csv("results")

    results = results.with_columns(
    pl.col("points").cast(pl.Float64, strict=False).fill_null(0.0)
)
    # nuovo anno = max(year) + 1 -> se max è 2024, new_year diventa 2025
    max_year = int(races.select(pl.col("year").max()).item())
    new_year = max_year + 1

    # base_year:
    # - se BASE_YEAR è impostato, lo rispettiamo
    # - altrimenti cloniamo l'ultimo anno disponibile (es. 2024)
    base_year_env = os.getenv("BASE_YEAR")
    base_year = int(base_year_env) if base_year_env else max_year

    # subset gare della stagione base
    base_races = (
        races
        .filter(pl.col("year") == base_year)
        .sort("round")
        .select(["raceId", "year", "round", "circuitId", "name", "date", "time", "url"])
    )
    if base_races.is_empty():
        raise ValueError(f"Nessuna gara trovata per base_year={base_year}")

    # calcolo nuovi raceId
    max_race_id = int(races.select(pl.col("raceId").max()).item())
    n_races = base_races.height
    new_race_ids = list(range(max_race_id + 1, max_race_id + 1 + n_races))

    # date plausibili: partiamo dal 1 marzo del new_year e aggiungiamo 7 giorni per round
    start_date = date(new_year, 3, 1)

    # costruisci nuove races (stesso round/circuit, anno nuovo)
    new_races = (
        base_races
        .with_columns([
            pl.Series("raceId", new_race_ids),
            pl.lit(new_year).alias("year"),

            # date: start_date + (round-1)*7 giorni
            (
                pl.lit(start_date)
                + (pl.col("round") - 1) * pl.duration(days=7)
            ).alias("date"),

            # name: se contiene "(YYYY)" lo sostituiamo, altrimenti appendiamo "(new_year)"
            pl.when(pl.col("name").cast(pl.Utf8).str.contains(r"\(\d{4}\)"))
              .then(pl.col("name").cast(pl.Utf8).str.replace(r"\(\d{4}\)", f"({new_year})"))
              .otherwise(pl.col("name").cast(pl.Utf8) + f" ({new_year})")
              .alias("name"),
        ])
        # time: spesso è sporca/mancante, teniamola a NULL
        .drop("time")
        .with_columns([pl.lit(None).alias("time")])
        .select(["raceId", "year", "round", "circuitId", "name", "date", "time", "url"])
    )

    # prendiamo i driver/constructor presenti in base_year dalla fact results
    base_race_ids = base_races.select("raceId").to_series().to_list()

    base_results_subset = (
        results
        .filter(pl.col("raceId").is_in(base_race_ids))
        .select(["driverId", "constructorId"])
        .unique()
    )

    # prendiamo 20 “entry” (driver+constructor) coerenti
    entries = base_results_subset.to_dicts()
    if len(entries) < 20:
        raise ValueError("Non ci sono abbastanza driver/constructor entries per generare 20 risultati per gara.")

    entries = random.sample(entries, k=20)

    # calcolo nuovi resultId
    max_result_id = int(results.select(pl.col("resultId").max()).item())

    # per ogni gara generiamo una classifica
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
                # statusId: 1 = Finished (semplice e coerente)
                "statusId": 1,
            })
            result_id_counter += 1

    new_results = pl.DataFrame(new_results_rows)

    # scrivi batch
    new_races.write_parquet(out_dir / "races.parquet")
    new_results.write_parquet(out_dir / "results.parquet")

    print(f"[OK] Synthetic season generated: year={new_year} from base_year={base_year} (dataset max_year={max_year})")
    print(f"[OK] Batch: {out_dir}")
    print(f"[OK] races rows={new_races.height}, results rows={new_results.height}")


if __name__ == "__main__":
    main()
