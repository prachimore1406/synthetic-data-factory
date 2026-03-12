from typing import Dict, Any
from ...relationships import parse_fk

def validate_contracts(context: Dict[str, Any]):
    cmc = context.get("cmc", {})
    if "entities" not in cmc or not isinstance(cmc.get("entities"), list):
        raise ValueError("invalid cmc")
    entities = cmc.get("entities", [])
    tables = set()
    cols_by_table = {}
    for e in entities:
        if not isinstance(e, dict):
            raise ValueError("invalid cmc entity")
        name = e.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("invalid cmc entity name")
        columns = e.get("columns")
        if not isinstance(columns, list) or not columns:
            raise ValueError("invalid cmc entity columns")
        tables.add(name)
        cols = set()
        for c in columns:
            if not isinstance(c, dict):
                raise ValueError("invalid cmc column")
            cn = c.get("name")
            ct = c.get("type")
            if not isinstance(cn, str) or not cn:
                raise ValueError("invalid cmc column name")
            if not isinstance(ct, str) or not ct:
                raise ValueError("invalid cmc column type")
            cols.add(cn)
        cols_by_table[name] = cols

    rels = cmc.get("relationships", [])
    if rels is None:
        rels = []
    if not isinstance(rels, list):
        raise ValueError("invalid cmc relationships")
    for rel in rels:
        if not isinstance(rel, dict):
            raise ValueError("invalid relationship")
        ft, tt, fc, tc = parse_fk(rel)
        if not ft or not tt:
            raise ValueError("invalid relationship endpoints")
        if ft not in tables or tt not in tables:
            raise ValueError("relationship table not found")
        if fc and fc not in cols_by_table.get(ft, set()):
            raise ValueError("relationship from_column not found")
        if tc and tc not in cols_by_table.get(tt, set()):
            raise ValueError("relationship to_column not found")
    return True
