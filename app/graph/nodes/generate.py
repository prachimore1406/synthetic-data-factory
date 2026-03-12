from typing import Dict, Any, List
import os
import random
import uuid
from decimal import Decimal

from ...rules import seed_int, infer_table_rules, domain_for, apply_row_rules, normalize_col
from ...relationships import parse_fk, topological_table_order

def _norm_type(t: str) -> str:
    base = str(t).replace("Nullable(", "").replace(")", "").strip()
    return base.lower()

def _default_string(table: str, col: str, row_index: int, rng: random.Random) -> str:
    c = normalize_col(col)
    if c in ["bu", "business_unit"]:
        return f"BU{(row_index % 5) + 1}"
    if c in ["curr", "currency"]:
        return ["USD", "EUR", "GBP", "JPY", "INR"][row_index % 5]
    if c in ["ledger"]:
        return ["primary", "secondary"][row_index % 2]
    if c.endswith("_id") or c == "id":
        ns = uuid.UUID(int=seed_int(table, col) % (1 << 128))
        return str(uuid.uuid5(ns, f"{row_index}"))
    return f"{c}_{row_index:06d}"

def _gen_value(table: str, col: str, t: str, row_index: int, value_domains: Dict[str, Any], rng: random.Random, row: Dict[str, Any]) -> Any:
    dom = domain_for(value_domains, table, col)
    if dom:
        dt = str(dom.get("type", "")).lower()
        if dt == "enum":
            values = dom.get("values", [])
            if isinstance(values, list) and values:
                return values[row_index % len(values)]
        if dt == "range":
            lo = dom.get("min")
            hi = dom.get("max")
            if isinstance(lo, (int, float, Decimal)) and isinstance(hi, (int, float, Decimal)) and hi >= lo:
                span = float(hi) - float(lo)
                return float(lo) + (span * (row_index % 100) / 100.0)
        if dt == "regex":
            pat = str(dom.get("pattern", ""))
            if pat.startswith("^") and len(pat) > 1:
                prefix = pat[1:].split("[", 1)[0].split("(", 1)[0]
                if prefix:
                    return f"{prefix}{row_index:06d}"

    tl = _norm_type(t)
    if "date" in tl and "time" not in tl:
        return f"2026-01-{(row_index % 28) + 1:02d}"
    if "timestamp" in tl or "datetime" in tl or "timestamptz" in tl:
        return f"2026-01-{(row_index % 28) + 1:02d}T{(row_index % 24):02d}:00:00Z"
    if any(x in tl for x in ["int", "bigint", "integer", "int64", "int32", "uint64"]):
        return row_index
    if any(x in tl for x in ["float", "double"]):
        return float(row_index) + 0.5
    if "decimal" in tl or "numeric" in tl:
        return Decimal(row_index) / Decimal("10")
    if "bool" in tl:
        return (row_index % 2) == 0
    return _default_string(table, col, row_index, rng)

def generate_rows(context: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    cmc = context["cmc"]
    rpc = context.get("rpc", {})
    value_domains = rpc.get("value_domains", {})
    if not isinstance(value_domains, dict):
        value_domains = {}

    row_count = int(os.getenv("SYNTH_ROW_COUNT", "50"))
    if isinstance(rpc.get("generation"), dict) and isinstance(rpc["generation"].get("row_count"), int):
        row_count = int(rpc["generation"]["row_count"])

    table_rules = infer_table_rules(cmc, rpc)
    schema_version = str(cmc.get("schema_version", "v0000"))
    relationships = cmc.get("relationships", []) or []
    rels = relationships if isinstance(relationships, list) else []

    rows: Dict[str, List[Dict[str, Any]]] = {}
    entities = cmc.get("entities", [])
    entity_map: Dict[str, Dict[str, Any]] = {}
    entity_names: List[str] = []
    for e in entities:
        if isinstance(e, dict) and isinstance(e.get("name"), str):
            entity_map[e["name"]] = e
            entity_names.append(e["name"])
    ordered_tables = topological_table_order(entity_names, rels)

    fk_by_from: Dict[str, List[Dict[str, str]]] = {}
    for rel in rels:
        if not isinstance(rel, dict):
            continue
        ft, tt, fc, tc = parse_fk(rel)
        if not ft or not tt or not fc or not tc:
            continue
        fk_by_from.setdefault(ft, []).append({"from_table": ft, "to_table": tt, "from_column": fc, "to_column": tc})

    value_pool: Dict[str, Dict[str, List[Any]]] = {}
    for table in ordered_tables:
        e = entity_map.get(table)
        if not e:
            continue
        cols = e.get("columns", [])
        if not isinstance(cols, list):
            continue
        col_meta = {c.get("name"): c for c in cols if isinstance(c, dict) and isinstance(c.get("name"), str)}
        rng = random.Random(seed_int(schema_version, table))
        data: List[Dict[str, Any]] = []
        for i in range(row_count):
            row: Dict[str, Any] = {}
            for n, c in col_meta.items():
                t = c.get("type", "String")
                row[n] = _gen_value(table, n, str(t), i, value_domains, rng, row)
            row = apply_row_rules(row, table_rules.get(table, {}), i, rng)
            for fk in fk_by_from.get(table, []):
                parent_table = fk["to_table"]
                parent_col = fk["to_column"]
                child_col = fk["from_column"]
                if child_col not in row:
                    continue
                child_def = col_meta.get(child_col, {})
                nullable = bool(child_def.get("nullable", True))
                parent_values = value_pool.get(parent_table, {}).get(parent_col, [])
                if not parent_values:
                    continue
                if nullable and (i % 10 == 0):
                    row[child_col] = None
                else:
                    row[child_col] = parent_values[i % len(parent_values)]
            data.append(row)
        rows[table] = data

        pool_cols = {}
        for n in col_meta.keys():
            pool_cols[n] = [r.get(n) for r in data if isinstance(r, dict)]
        value_pool[table] = pool_cols
    return rows
