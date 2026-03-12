from typing import Dict, Any
from langgraph.graph import StateGraph, END
from .nodes.validate import validate_contracts
from .nodes.emit_ddl import emit_ddl
from .nodes.decide import decide_action
from .nodes.generate import generate_rows
from .nodes.insert import bulk_insert
from .nodes.export_csv import export_rows_to_csv
from .nodes.qa import qa_checks
from pathlib import Path
import json

def node_validate(state: Dict[str, Any]) -> Dict[str, Any]:
    validate_contracts(state)
    return {"state": "validate", "progress": 10}

def node_emit_ddl(state: Dict[str, Any]) -> Dict[str, Any]:
    ddl = emit_ddl(state)
    tables = list(ddl.keys())
    return {"ddl": ddl, "tables": tables, "state": "emit_ddl", "progress": 30}

def node_decide(state: Dict[str, Any]) -> Dict[str, Any]:
    return decide_action(state)

def node_gate(state: Dict[str, Any]) -> Dict[str, Any]:
    approved = state.get("ddl_approved", True)
    if approved:
        return {"state": "gate_passed", "progress": 40, "halted": False}
    return {"state": "gate_wait", "progress": 40, "halted": True}

def node_generate(state: Dict[str, Any]) -> Dict[str, Any]:
    rows = generate_rows(state)
    run_dir = Path(state["run_dir"])
    out_path = run_dir / "rows.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2, default=str)
    inserted_preview = {k: len(v) for k, v in rows.items() if isinstance(v, list)}
    return {"rows_path": str(out_path), "rows_preview": inserted_preview, "state": "generate_rows", "progress": 60}

def node_insert(state: Dict[str, Any]) -> Dict[str, Any]:
    rows = {}
    rows_path = state.get("rows_path")
    if isinstance(rows_path, str) and rows_path:
        p = Path(rows_path)
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                rows = json.load(f)
    inserted = bulk_insert(state, rows)
    return {"inserted": inserted, "state": "insert", "progress": 80}

def node_export_csv(state: Dict[str, Any]) -> Dict[str, Any]:
    rows = {}
    rows_path = state.get("rows_path")
    if isinstance(rows_path, str) and rows_path:
        p = Path(rows_path)
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                rows = json.load(f)
    res = export_rows_to_csv(state, rows)
    return {**res, "state": "export_csv", "progress": 85}

def node_qa(state: Dict[str, Any]) -> Dict[str, Any]:
    rows = {}
    rows_path = state.get("rows_path")
    if isinstance(rows_path, str) and rows_path:
        p = Path(rows_path)
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                rows = json.load(f)
    state_with_rows = dict(state)
    state_with_rows["rows"] = rows
    report = qa_checks(state_with_rows)
    return {"qa_report_url": report, "state": "qa", "progress": 95}

def node_complete(state: Dict[str, Any]) -> Dict[str, Any]:
    return {"state": "completed", "progress": 100}

def build_graph():
    graph = StateGraph(dict)
    graph.add_node("validate", node_validate)
    graph.add_node("emit_ddl", node_emit_ddl)
    graph.add_node("decide", node_decide)
    graph.add_node("gate", node_gate)
    graph.add_node("generate_rows", node_generate)
    graph.add_node("insert", node_insert)
    graph.add_node("export_csv", node_export_csv)
    graph.add_node("qa", node_qa)
    graph.add_node("complete", node_complete)
    graph.set_entry_point("validate")
    graph.add_edge("validate", "emit_ddl")
    graph.add_conditional_edges(
        "emit_ddl",
        lambda s: "csv" if s.get("sink") == "csv" else "postgres",
        {"csv": "generate_rows", "postgres": "decide"},
    )
    graph.add_conditional_edges(
        "decide",
        lambda s: "stop" if s.get("halted") else "continue",
        {"stop": END, "continue": "gate"},
    )
    graph.add_conditional_edges(
        "gate",
        lambda s: "halt" if s.get("halted") else "continue",
        {"halt": END, "continue": "generate_rows"},
    )
    graph.add_conditional_edges(
        "generate_rows",
        lambda s: "insert" if s.get("sink") == "postgres" else "export",
        {"insert": "insert", "export": "export_csv"},
    )
    graph.add_edge("insert", "export_csv")
    graph.add_edge("export_csv", "qa")
    graph.add_edge("qa", "complete")
    graph.add_edge("complete", END)
    return graph.compile()
