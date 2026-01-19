from etl.tasks.bronze_fase2 import load_bronze_f1_incremental
from etl.tasks.silver_fase2 import build_silver_f1
from etl.tasks.gold_fase2 import build_gold_f1

def main():
    processed = load_bronze_f1_incremental.fn(raw_root="data_lake/raw")
    silver = build_silver_f1.fn()
    gold = build_gold_f1.fn()
    print({"processed_dt": processed, "silver": silver, "gold": gold})

if __name__ == "__main__":
    main()
