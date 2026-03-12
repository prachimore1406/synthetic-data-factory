from typing import Dict, Any
from ...relationships import fk_ddl_statements, topological_table_order

def _map_type(t: str) -> str:
    base = t.replace("Nullable(", "").replace(")", "")
    base_l = base.lower()
    if base_l in ["string", "lowcardinality(string)", "text", "varchar"]:
        return "text"
    if base_l in ["date"]:
        return "date"
    if base_l in ["datetime", "timestamp", "timestamptz"]:
        return "timestamp"
    if base_l in ["int", "int32", "integer"]:
        return "integer"
    if base_l in ["int64", "bigint", "uint64"]:
        return "bigint"
    if base_l in ["float32", "float64", "float", "double"]:
        return "double precision"
    if base_l in ["boolean", "bool"]:
        return "boolean"
    return base_l

def emit_ddl(context: Dict[str, Any]) -> Dict[str, str]:
    cmc = context["cmc"]
    db_name = context["db_name"]
    out: Dict[str, str] = {}
    entities = cmc.get("entities", [])
    rels = cmc.get("relationships", []) or []
    name_by_entity = []
    entity_map = {}
    for e in entities:
        if isinstance(e, dict) and isinstance(e.get("name"), str):
            name_by_entity.append(e["name"])
            entity_map[e["name"]] = e
    ordered = topological_table_order(name_by_entity, rels if isinstance(rels, list) else [])
    for name in ordered:
        e = entity_map.get(name)
        if not isinstance(e, dict):
            continue
        cols = []
        for c in e["columns"]:
            t = _map_type(c["type"])
            if not c.get("nullable", True):
                t = f"{t} NOT NULL"
            cols.append(f"{c['name']} {t}")
        cols_sql = ",\n  ".join(cols)
        sql = f'CREATE TABLE IF NOT EXISTS "{db_name}"."{e["name"]}" (\n  {cols_sql}\n);'
        out[e["name"]] = sql
    for cname, sql in fk_ddl_statements(db_name, rels if isinstance(rels, list) else []):
        out[cname] = sql
    return out
