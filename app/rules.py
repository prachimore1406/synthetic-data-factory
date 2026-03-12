from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import hashlib
import random
import re

_RE_VALUE = re.compile(r"(?:starts with|prefix|value)\s*['\"]?([a-z0-9_]+)['\"]?", re.IGNORECASE)
_RE_ASSIGN = re.compile(r"([a-z0-9_]+)\s*(?:=|equals|derived from|same as|will be|is)\s*([a-z0-9_]+)", re.IGNORECASE)
_RE_FIRST_FIELD_VALUE = re.compile(r"first field.*?value\s*['\"]?([a-z0-9_]+)['\"]?", re.IGNORECASE)

def seed_int(*parts: str) -> int:
    h = hashlib.sha256("|".join(parts).encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big", signed=False)

def normalize_col(name: str) -> str:
    return name.strip().lower()

def _columns_by_table(cmc: Dict[str, Any]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for e in cmc.get("entities", []):
        t = e.get("name")
        if not isinstance(t, str):
            continue
        cols = []
        for c in e.get("columns", []):
            n = c.get("name")
            if isinstance(n, str):
                cols.append(n)
        out[t] = cols
    return out

def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9_]+", text.lower())

def _mentions_column(tokens: List[str], col_norm: str) -> bool:
    tset = set(tokens)
    if col_norm in tset:
        return True
    if "_" in col_norm:
        parts = [p for p in col_norm.split("_") if p]
        if parts and all(p in tset for p in parts):
            return True
    return False

def _resolve_token_to_col(token: str, col_norms: List[str]) -> Optional[str]:
    tok = normalize_col(token)
    if tok in col_norms:
        return tok
    for c in col_norms:
        if "_" in c and tok in c.split("_"):
            return c
    return None

def infer_table_rules(cmc: Dict[str, Any], rpc: Dict[str, Any]) -> Dict[str, Dict[str, Dict[str, str]]]:
    cols_by_table = _columns_by_table(cmc)
    rules_by_table: Dict[str, Dict[str, Dict[str, str]]] = {}
    for t in cols_by_table:
        rules_by_table[t] = {"equals": {}, "prefix": {}}

    rules = rpc.get("rules", [])
    if not isinstance(rules, list):
        return rules_by_table

    for r in rules:
        if not isinstance(r, dict):
            continue
        r_type = str(r.get("type", "")).lower()
        r_table = r.get("table") or r.get("entity") or r.get("table_name")
        r_target = r.get("target") or r.get("column") or r.get("field")
        r_source = r.get("source") or r.get("from") or r.get("equals")
        r_value = r.get("value") or r.get("prefix") or r.get("starts_with")

        predicate = str(r.get("predicate", "")).strip()
        pred_l = predicate.lower()
        tokens = _tokenize(predicate) if predicate else []

        for table, cols in cols_by_table.items():
            col_norms = [normalize_col(c) for c in cols]

            if r_table and isinstance(r_table, str) and r_table != table:
                continue

            if r_type == "equals" and isinstance(r_target, str) and isinstance(r_source, str):
                t_col = _resolve_token_to_col(r_target, col_norms)
                s_col = _resolve_token_to_col(r_source, col_norms)
                if t_col and s_col:
                    rules_by_table[table]["equals"][t_col] = s_col

            if r_type == "prefix" and isinstance(r_target, str) and r_value is not None:
                t_col = _resolve_token_to_col(str(r_target), col_norms)
                if t_col:
                    rules_by_table[table]["prefix"][t_col] = str(r_value)

            if not predicate:
                continue

            m2 = _RE_ASSIGN.search(predicate)
            if m2:
                a = _resolve_token_to_col(m2.group(1), col_norms)
                b = _resolve_token_to_col(m2.group(2), col_norms)
                if a and b:
                    rules_by_table[table]["equals"][a] = b

            wants_prefix = any(s in pred_l for s in ["starts with", "prefix", "first field"])
            if wants_prefix:
                m = _RE_VALUE.search(predicate) or _RE_FIRST_FIELD_VALUE.search(predicate)
                if not m:
                    continue
                prefix = str(m.group(1))
                mentioned = [c for c in col_norms if _mentions_column(tokens, c)]
                if len(mentioned) == 1:
                    rules_by_table[table]["prefix"][mentioned[0]] = prefix

    return rules_by_table

def domain_for(value_domains: Dict[str, Any], table: str, column: str) -> Optional[Dict[str, Any]]:
    key1 = f"{table}.{column}"
    if key1 in value_domains and isinstance(value_domains[key1], dict):
        return value_domains[key1]
    if column in value_domains and isinstance(value_domains[column], dict):
        return value_domains[column]
    col_u = column.upper()
    if col_u in value_domains and isinstance(value_domains[col_u], dict):
        return value_domains[col_u]
    return None

def apply_row_rules(row: Dict[str, Any], table_rules: Dict[str, Dict[str, str]], row_index: int, rng: random.Random) -> Dict[str, Any]:
    key_map = {normalize_col(k): k for k in row.keys()}
    equals = table_rules.get("equals", {})
    for target, source in equals.items():
        source_k = key_map.get(normalize_col(source))
        target_k = key_map.get(normalize_col(target))
        if source_k is None or target_k is None:
            continue
        row[target_k] = row[source_k]

    prefix = table_rules.get("prefix", {})
    for target, p in prefix.items():
        target_k = key_map.get(normalize_col(target))
        if target_k is None:
            continue
        existing = row.get(target_k)
        if isinstance(existing, str) and existing:
            if existing.startswith(str(p)):
                row[target_k] = existing
            else:
                row[target_k] = f"{p}{existing}"
        else:
            row[target_k] = f"{p}{row_index:06d}"

    return row

def evaluate_row_rules(row: Dict[str, Any], table_rules: Dict[str, Dict[str, str]]) -> List[Dict[str, Any]]:
    violations: List[Dict[str, Any]] = []
    key_map = {normalize_col(k): k for k in row.keys()}
    equals = table_rules.get("equals", {})
    for target, source in equals.items():
        source_k = key_map.get(normalize_col(source))
        target_k = key_map.get(normalize_col(target))
        if source_k is None or target_k is None:
            continue
        if row[target_k] != row[source_k]:
            violations.append({"type": "equals", "target": target_k, "source": source_k})

    prefix = table_rules.get("prefix", {})
    for target, p in prefix.items():
        target_k = key_map.get(normalize_col(target))
        if target_k is None:
            continue
        v = row.get(target_k)
        if v is None:
            continue
        s = str(v)
        if not s.startswith(str(p)):
            violations.append({"type": "prefix", "target": target_k, "prefix": str(p)})
    return violations

