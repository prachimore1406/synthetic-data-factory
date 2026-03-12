from typing import Dict, Any, List
from pathlib import Path
import json

from ...rules import infer_table_rules, evaluate_row_rules, domain_for
from ...relationships import parse_fk

def qa_checks(context: Dict[str, Any]) -> str:
    run_dir = Path(context["run_dir"])
    cmc = context.get("cmc", {})
    rpc = context.get("rpc", {})
    rows = context.get("rows", {})
    value_domains = rpc.get("value_domains", {})
    if not isinstance(value_domains, dict):
        value_domains = {}

    rules_by_table = infer_table_rules(cmc, rpc)
    violations: Dict[str, Any] = {"rules": {}, "value_domains": {}, "relationships": {}}
    total_violations = 0

    rels = cmc.get("relationships", []) or []
    rel_list = rels if isinstance(rels, list) else []
    fk_refs: List[Dict[str, str]] = []
    for rel in rel_list:
        if not isinstance(rel, dict):
            continue
        ft, tt, fc, tc = parse_fk(rel)
        if ft and tt and fc and tc:
            fk_refs.append({"from_table": ft, "to_table": tt, "from_column": fc, "to_column": tc})

    if isinstance(rows, dict) and fk_refs:
        parent_sets: Dict[str, Dict[str, set]] = {}
        for fk in fk_refs:
            tt = fk["to_table"]
            tc = fk["to_column"]
            data = rows.get(tt, [])
            if not isinstance(data, list):
                continue
            parent_sets.setdefault(tt, {})
            if tc not in parent_sets[tt]:
                parent_sets[tt][tc] = set()
                for r in data:
                    if isinstance(r, dict) and r.get(tc) is not None:
                        parent_sets[tt][tc].add(r.get(tc))

        for fk in fk_refs:
            ft = fk["from_table"]
            tt = fk["to_table"]
            fc = fk["from_column"]
            tc = fk["to_column"]
            data = rows.get(ft, [])
            if not isinstance(data, list):
                continue
            allowed = parent_sets.get(tt, {}).get(tc, set())
            bad: List[Dict[str, Any]] = []
            for idx, r in enumerate(data):
                if not isinstance(r, dict):
                    continue
                v = r.get(fc)
                if v is None:
                    continue
                if v not in allowed:
                    bad.append({"row": idx, "from_column": fc, "to_table": tt, "to_column": tc})
            if bad:
                key = f"{ft}.{fc}->{tt}.{tc}"
                violations["relationships"][key] = bad[:200]
                total_violations += len(bad)

    for table, data in rows.items() if isinstance(rows, dict) else []:
        if not isinstance(table, str) or not isinstance(data, list):
            continue
        table_rules = rules_by_table.get(table, {})
        bad_rules: List[Dict[str, Any]] = []
        bad_domains: List[Dict[str, Any]] = []
        for idx, row in enumerate(data):
            if not isinstance(row, dict):
                continue
            for v in evaluate_row_rules(row, table_rules):
                bad_rules.append({"row": idx, **v})
            for k, v in row.items():
                dom = domain_for(value_domains, table, str(k))
                if not dom:
                    continue
                dt = str(dom.get("type", "")).lower()
                if dt == "enum":
                    values = dom.get("values", [])
                    if isinstance(values, list) and values and v not in values:
                        bad_domains.append({"row": idx, "column": k, "type": "enum"})
                elif dt == "regex":
                    pat = str(dom.get("pattern", ""))
                    try:
                        import re
                        if pat and not re.match(pat, str(v)):
                            bad_domains.append({"row": idx, "column": k, "type": "regex"})
                    except Exception:
                        pass
                elif dt == "range":
                    lo = dom.get("min")
                    hi = dom.get("max")
                    try:
                        fv = float(v)
                        if lo is not None and fv < float(lo):
                            bad_domains.append({"row": idx, "column": k, "type": "range"})
                        if hi is not None and fv > float(hi):
                            bad_domains.append({"row": idx, "column": k, "type": "range"})
                    except Exception:
                        pass
        if bad_rules:
            violations["rules"][table] = bad_rules[:200]
            total_violations += len(bad_rules)
        if bad_domains:
            violations["value_domains"][table] = bad_domains[:200]
            total_violations += len(bad_domains)

    report = {
        "status": "ok" if total_violations == 0 else "failed",
        "total_violations": total_violations,
        "violations": violations,
    }
    out = run_dir / "qa_report.json"
    with out.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return str(out)
