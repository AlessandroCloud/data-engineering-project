import json
from pathlib import Path
from typing import Set

def load_processed_dt(ledger_path: Path) -> Set[str]:
    if not ledger_path.exists():
        return set()
    obj = json.loads(ledger_path.read_text(encoding="utf-8"))
    return set(obj.get("processed_dt", []))

def mark_dt_processed(ledger_path: Path, dt: str) -> None:
    obj = {"processed_dt": []}
    if ledger_path.exists():
        obj = json.loads(ledger_path.read_text(encoding="utf-8"))

    s = set(obj.get("processed_dt", []))
    s.add(dt)
    obj["processed_dt"] = sorted(s)

    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
