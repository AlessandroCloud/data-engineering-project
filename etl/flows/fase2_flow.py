from __future__ import annotations

from prefect import flow

from etl.tasks.bronze_fase2 import load_bronze_f1_incremental
from etl.tasks.silver_fase2 import build_silver_f1
from etl.tasks.gold_fase2 import build_gold_f1


@flow(name="fase2_f1_end_to_end")
def fase2_flow(raw_root: str = "data_lake/raw"):
    """
    Flow unico Fase 2:
    1) Bronze incrementale (dt non ancora processati)
    2) Silver rebuild deterministico (dedup + QC)
    3) Gold rebuild (star schema + QC)
    """
    processed_dts = load_bronze_f1_incremental(raw_root=raw_root)
    built_silver = build_silver_f1()
    built_gold = build_gold_f1()

    print("Processed dt:", processed_dts)
    print("Built silver:", built_silver)
    print("Built gold:", built_gold)

    return {
        "processed_dt": processed_dts,
        "silver_tables": built_silver,
        "gold_tables": built_gold,
    }


if __name__ == "__main__":
    fase2_flow()
