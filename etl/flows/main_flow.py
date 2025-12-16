from prefect import flow
from etl.tasks.bronze import load_bronze_f1

@flow(name="f1_pipeline")
def main_flow():
    n = load_bronze_f1()
    print(f"[OK] Bronze caricato: {n} tabelle")
