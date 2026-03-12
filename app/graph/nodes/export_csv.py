from typing import Dict, Any, List
from pathlib import Path
import csv


def export_rows_to_csv(context: Dict[str, Any], rows: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    run_dir = Path(context["run_dir"])
    out_dir = run_dir / "csv"
    out_dir.mkdir(parents=True, exist_ok=True)

    exported: Dict[str, int] = {}
    files: Dict[str, str] = {}

    cmc = context.get("cmc", {})
    cmc_entities = {e.get("name"): e for e in cmc.get("entities", []) if isinstance(e, dict)}

    for table, data in rows.items():
        if not isinstance(table, str) or not isinstance(data, list):
            continue
        entity = cmc_entities.get(table, {})
        cols = entity.get("columns", [])
        if isinstance(cols, list) and cols:
            col_names = [c.get("name") for c in cols if isinstance(c, dict) and isinstance(c.get("name"), str)]
        else:
            col_names = list(data[0].keys()) if data and isinstance(data[0], dict) else []
        if not col_names:
            continue

        p = out_dir / f"{table}.csv"
        with p.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(col_names)
            count = 0
            for row in data:
                if not isinstance(row, dict):
                    continue
                w.writerow([row.get(c) for c in col_names])
                count += 1
        exported[table] = count
        files[table] = str(p)

    return {"csv_dir": str(out_dir), "csv_files": files, "exported": exported}

