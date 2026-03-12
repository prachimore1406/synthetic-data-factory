from typing import Dict, Any, List, Tuple
import os
from ...db.postgres_io import try_connect, list_tables, get_table_columns

def _expected_schema(cmc: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    expected = {}
    for e in cmc.get("entities", []):
        cols = {}
        for c in e.get("columns", []):
            cols[c["name"]] = _map_type(c["type"])
        expected[e["name"]] = cols
    return expected

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

def _diff_schema(expected: Dict[str, Dict[str, str]], existing: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    additions = {"tables": [], "columns": []}
    breaking = {"tables": [], "columns": []}
    for table, cols in expected.items():
        if table not in existing:
            additions["tables"].append(table)
            continue
        for col, col_type in cols.items():
            if col not in existing[table]:
                additions["columns"].append({"table": table, "column": col})
            elif existing[table][col] != col_type:
                breaking["columns"].append({"table": table, "column": col, "existing": existing[table][col], "expected": col_type})
    for table, cols in existing.items():
        if table not in expected:
            breaking["tables"].append(table)
        else:
            for col in cols:
                if col not in expected[table]:
                    breaking["columns"].append({"table": table, "column": col, "existing": cols[col], "expected": None})
    return {"additions": additions, "breaking": breaking}

def _decide_with_crewai(diff: Dict[str, Any]) -> Tuple[str, str]:
    try:
        from crewai import Agent, Task, Crew
    except Exception:
        return "", ""
    decider = Agent(
        role="Schema Diff Decider",
        goal="Choose append or block based on schema diffs",
        backstory="Prioritizes data integrity",
        allow_delegation=False,
        verbose=False,
    )
    t1 = Task(
        description=f"Given schema diff JSON, decide action: append or block. Respond with JSON: {{\"decision\":\"append|block\",\"reason\":\"...\"}}.\nDiff:\n{diff}",
        expected_output='{"decision":"append","reason":"..."}',
        agent=decider,
    )
    crew = Crew(agents=[decider], tasks=[t1], verbose=False)
    out = crew.kickoff()
    try:
        import json
        data = json.loads(str(out))
        return data.get("decision", ""), data.get("reason", "")
    except Exception:
        return "", ""

def decide_action(state: Dict[str, Any]) -> Dict[str, Any]:
    if state.get("sink") == "csv":
        return {
            "decision": "append",
            "decision_reason": "csv sink; skipping Postgres schema diff",
            "schema_diff": None,
            "state": "decision_append",
            "progress": 35,
            "ddl_approved": True,
            "halted": False,
        }
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    username = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    database = os.getenv("POSTGRES_DB", "postgres")
    client = try_connect(host, port, username, password, database)
    expected = _expected_schema(state["cmc"])
    existing = {}
    for table in list_tables(client, state["db_name"]):
        existing[table] = get_table_columns(client, state["db_name"], table)
    diff = _diff_schema(expected, existing)
    decision, reason = _decide_with_crewai(diff)
    if decision not in ["append", "block"]:
        if diff["breaking"]["tables"] or diff["breaking"]["columns"]:
            decision = "block"
            reason = "breaking schema differences detected"
        else:
            decision = "append"
            reason = "only additive changes"
    update = {
        "decision": decision,
        "decision_reason": reason,
        "schema_diff": diff,
    }
    if decision == "block":
        update["state"] = "blocked"
        update["progress"] = 35
        update["halted"] = True
    else:
        update["state"] = "decision_append"
        update["progress"] = 35
        update["ddl_approved"] = bool(state.get("ddl_approved", False))
        update["halted"] = False
    return update
