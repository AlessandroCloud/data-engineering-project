import os
from pathlib import Path
from datetime import date

import pandas as pd

# Batch date (simulazione data di arrivo)
BATCH_DT = os.getenv("BATCH_DT", str(date.today()))

SRC = Path(os.getenv("RAW_SRC_PATH", "data/raw/f1"))
OUT = Path(os.getenv("DATA_LAKE_PATH", "data_lake")) / "raw" / f"dt={BATCH_DT}"
OUT.mkdir(parents=True, exist_ok=True)

# Set minimo per far funzionare il tuo star schema Fase 1
csv_files = [
    "races.csv",
    "results.csv",
    "drivers.csv",
    "constructors.csv",
    "circuits.csv",
    "status.csv",
]

print("Source:", SRC.resolve())
print("Output:", OUT.resolve())

for name in csv_files:
    p = SRC / name
    if not p.exists():
        print(f"[SKIP] Missing: {p}")
        continue

    df = pd.read_csv(p)
    out_path = OUT / name.replace(".csv", ".parquet")
    df.to_parquet(out_path, index=False)
    print("[OK]", name, "->", out_path.name)

print("Batch created:", OUT)
