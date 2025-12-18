from prefect import flow
from etl.tasks.bronze import load_bronze_f1
from etl.tasks.silver import build_silver
from etl.tasks.gold import build_gold

@flow(name="f1_pipeline")
def main_flow():
    n = load_bronze_f1()
    print(f"[OK] Bronze caricato: {n} tabelle")

    silver_summary = build_silver()
    print("[OK] Silver creato.")

    gold_summary = build_gold()
    print("[OK] Gold creato. Righe per tabella:")
    for k, v in gold_summary.items():
        print(f" - {k}: {v}")
