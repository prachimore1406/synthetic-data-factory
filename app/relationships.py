from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple
import re

_RE_FK_ARROW = re.compile(r"^\s*([a-zA-Z0-9_\.]+)\s*->\s*([a-zA-Z0-9_\.]+)\s*$")
_RE_FK_COLONLY = re.compile(r"^\s*([a-zA-Z0-9_]+)\s*$")

def _norm(s: str) -> str:
    return s.strip()

def _split_qual(name: str) -> Tuple[Optional[str], str]:
    s = _norm(name)
    if "." in s:
        a, b = s.split(".", 1)
        return a, b
    return None, s

def parse_fk(rel: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    from_table = rel.get("from") or rel.get("from_table") or rel.get("source_table") or rel.get("from_entity")
    to_table = rel.get("to") or rel.get("to_table") or rel.get("target_table") or rel.get("to_entity")
    if not isinstance(from_table, str) or not isinstance(to_table, str):
        return None, None, None, None
    from_table = _norm(from_table)
    to_table = _norm(to_table)

    fk = rel.get("fk")
    if isinstance(fk, dict):
        fc = fk.get("from_column") or fk.get("from") or fk.get("column") or fk.get("source_column")
        tc = fk.get("to_column") or fk.get("to") or fk.get("ref_column") or fk.get("target_column")
        if isinstance(fc, str) and isinstance(tc, str):
            return from_table, to_table, _norm(fc), _norm(tc)
        return from_table, to_table, None, None

    if isinstance(fk, str) and fk.strip():
        m = _RE_FK_ARROW.match(fk)
        if m:
            left = m.group(1)
            right = m.group(2)
            lt, lc = _split_qual(left)
            rt, rc = _split_qual(right)
            if lt and lt != from_table:
                from_table = lt
            if rt and rt != to_table:
                to_table = rt
            return from_table, to_table, lc, rc
        m2 = _RE_FK_COLONLY.match(fk)
        if m2:
            col = m2.group(1)
            return from_table, to_table, col, col

    fc = rel.get("from_column") or rel.get("source_column") or rel.get("column")
    tc = rel.get("to_column") or rel.get("target_column") or rel.get("ref_column")
    if isinstance(fc, str) and isinstance(tc, str):
        return from_table, to_table, _norm(fc), _norm(tc)

    return from_table, to_table, None, None

def topological_table_order(entities: Sequence[str], relationships: Sequence[Dict[str, Any]]) -> List[str]:
    tables = [t for t in entities if isinstance(t, str)]
    deps: Dict[str, set[str]] = {t: set() for t in tables}
    rev: Dict[str, set[str]] = {t: set() for t in tables}

    for rel in relationships:
        if not isinstance(rel, dict):
            continue
        ft, tt, _, _ = parse_fk(rel)
        if not ft or not tt:
            continue
        if ft in deps and tt in deps and ft != tt:
            deps[ft].add(tt)
            rev[tt].add(ft)

    ready = [t for t in tables if not deps[t]]
    out: List[str] = []
    while ready:
        n = ready.pop(0)
        out.append(n)
        for child in list(rev[n]):
            deps[child].discard(n)
            if not deps[child]:
                ready.append(child)
        rev[n].clear()

    if len(out) != len(tables):
        seen = set(out)
        out.extend([t for t in tables if t not in seen])
    return out

def fk_ddl_statements(db_name: str, relationships: Sequence[Dict[str, Any]]) -> List[Tuple[str, str]]:
    stmts: List[Tuple[str, str]] = []
    i = 0
    for rel in relationships:
        if not isinstance(rel, dict):
            continue
        ft, tt, fc, tc = parse_fk(rel)
        if not ft or not tt or not fc or not tc:
            continue
        i += 1
        cname = f"fk_{ft}_{fc}_{i}"
        sql = (
            f'ALTER TABLE "{db_name}"."{ft}" '
            f'ADD CONSTRAINT "{cname}" FOREIGN KEY ("{fc}") '
            f'REFERENCES "{db_name}"."{tt}" ("{tc}");'
        )
        stmts.append((cname, sql))
    return stmts

