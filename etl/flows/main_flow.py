from prefect import flow
from etl.tasks.bronze import load_bronze_f1
from etl.tasks.silver import build_silver

@flow(name="f1_pipeline")
def main_flow():
    n = load_bronze_f1()
    print(f"[OK] Bronze caricato: {n} tabelle")

    silver_summary = build_silver()
    print("[OK] Silver creato. Righe per tabella:")
    for k, v in silver_summary.items():
        print(f" - {k}: {v}")
