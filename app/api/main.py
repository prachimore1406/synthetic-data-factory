from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ValidationError
from typing import Optional, List, Dict, Any, Union
from uuid import uuid4
from pathlib import Path
from datetime import datetime
import json
import os
import asyncio
from app.relationships import fk_ddl_statements, topological_table_order
try:
    from app.config import load_env
except Exception:
    from config import load_env
load_env()
try:
    from app.crewai.crew import generate_proposal
except Exception:
    from crewai.crew import generate_proposal
try:
    from app.graph.graph import build_graph
except Exception:
    from graph.graph import build_graph

ART_ROOT = Path(__file__).resolve().parents[2] / "artifacts"
FROZEN_SCHEMA_DIR = ART_ROOT / "frozen" / "schema"
FROZEN_RULEPACK_DIR = ART_ROOT / "frozen" / "rulepack"
RUNS_DIR = ART_ROOT / "runs"

for d in [FROZEN_SCHEMA_DIR, FROZEN_RULEPACK_DIR, RUNS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

class Column(BaseModel):
    name: str
    type: str
    nullable: Optional[bool] = True
    semantic: Optional[str] = None

class Entity(BaseModel):
    name: str
    columns: List[Column]

class Relationship(BaseModel):
    from_: str = Field(alias="from")
    to: str
    type: str
    fk: Optional[Union[str, Dict[str, str]]] = None

class CMC(BaseModel):
    schema_version: str
    domain_label: str
    entities: List[Entity]
    relationships: List[Relationship] = Field(default_factory=list)
    nl_sql_hints: Dict[str, List[str]] = Field(default_factory=dict)

class Rule(BaseModel):
    id: str
    type: str
    predicate: str
    severity: str

class RPC(BaseModel):
    schema_version: str
    rules: List[Rule] = Field(default_factory=list)
    naming: Dict[str, str] = Field(default_factory=dict)
    value_domains: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

class ChatRequest(BaseModel):
    session_id: Optional[str] = None
    message: str

class ChatResponse(BaseModel):
    session_id: str
    assistant: List[str]
    proposal: Optional[Dict[str, Any]] = None

class FreezeRequest(BaseModel):
    session_id: str
    cmc: Dict[str, Any]
    rpc: Dict[str, Any]

class FreezeResponse(BaseModel):
    schema_version: str
    frozen_at: str
    hashes: Dict[str, str]

class RunRequest(BaseModel):
    schema_version: str
    db_prefix: Optional[str] = "synthetic"
    sink: Optional[str] = "postgres"

class RunResponse(BaseModel):
    run_id: str
    status: str

class StatusResponse(BaseModel):
    state: str
    progress: int
    db_name: Optional[str] = None
    sink: Optional[str] = None
    tables: List[str] = Field(default_factory=list)
    qa_report_url: Optional[str] = None
    ddl: Optional[Dict[str, str]] = None
    ddl_approved: Optional[bool] = None
    decision: Optional[str] = None
    decision_reason: Optional[str] = None
    schema_diff: Optional[Dict[str, Any]] = None
    rows_preview: Optional[Dict[str, int]] = None
    inserted: Optional[Dict[str, int]] = None
    csv_dir: Optional[str] = None
    csv_files: Optional[Dict[str, str]] = None
    exported: Optional[Dict[str, int]] = None

class VersionListResponse(BaseModel):
    versions: List[str] = Field(default_factory=list)

class RunSummary(BaseModel):
    run_id: str
    schema_version: Optional[str] = None
    db_name: Optional[str] = None
    sink: Optional[str] = None
    state: Optional[str] = None
    progress: Optional[int] = None
    created_at: Optional[str] = None

class RunListResponse(BaseModel):
    runs: List[RunSummary] = Field(default_factory=list)

class ContractsResponse(BaseModel):
    cmc: Dict[str, Any]
    rpc: Dict[str, Any]

app = FastAPI(title="Synthetic Data Factory API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SESSIONS: Dict[str, Dict[str, Any]] = {}
RUNS: Dict[str, Dict[str, Any]] = {}

def next_schema_version() -> str:
    versions = []
    for p in FROZEN_SCHEMA_DIR.glob("v*.json"):
        try:
            versions.append(int(p.stem.replace("v", "")))
        except:
            pass
    n = max(versions) + 1 if versions else 1
    return f"v{n:04d}"

def save_json(path: Path, data: Dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return str(path)

def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def ddl_from_cmc(cmc: CMC, db_name: str) -> Dict[str, str]:
    def map_type(t: str) -> str:
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
    out: Dict[str, str] = {}
    entity_names = [e.name for e in cmc.entities]
    rels = []
    for r in cmc.relationships:
        rels.append({"from": r.from_, "to": r.to, "type": r.type, "fk": r.fk})
    ordered = topological_table_order(entity_names, rels)
    entity_map = {e.name: e for e in cmc.entities}
    for name in ordered:
        e = entity_map.get(name)
        if e is None:
            continue
        cols = []
        for c in e.columns:
            t = map_type(c.type)
            if not c.nullable:
                t = f"{t} NOT NULL"
            cols.append(f"{c.name} {t}")
        cols_sql = ",\n  ".join(cols)
        sql = f'CREATE TABLE IF NOT EXISTS "{db_name}"."{e.name}" (\n  {cols_sql}\n);'
        out[e.name] = sql
    for cname, sql in fk_ddl_statements(db_name, rels):
        out[cname] = sql
    return out

async def pipeline_run(run_id: str):
    run = RUNS[run_id]
    if run.get("state") == "completed":
        return
    cmc_path = FROZEN_SCHEMA_DIR / f"{run['schema_version']}.json"
    rpc_path = FROZEN_RULEPACK_DIR / f"{run['schema_version']}.json"
    state = {
        "cmc": load_json(cmc_path),
        "rpc": load_json(rpc_path),
        "db_name": run["db_name"],
        "run_dir": run["dir"],
        "ddl_approved": run.get("ddl_approved", True),
        "sink": run.get("sink", "postgres"),
        "state": "started",
        "progress": 0,
    }
    graph = build_graph()
    for update in graph.stream(state):
        if isinstance(update, dict):
            run.update(update)
        save_json(Path(run["dir"]) / "manifest.json", run)
        await asyncio.sleep(0.05)

@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    sid = req.session_id or str(uuid4())
    convo = SESSIONS.get(sid, {"messages": [], "proposal": None})
    convo["messages"].append({"role": "user", "text": req.message})
    assistant_msgs, cmc, rpc = generate_proposal(req.message, convo["messages"])
    convo["proposal"] = {"cmc": cmc, "rpc": rpc}
    SESSIONS[sid] = convo
    return ChatResponse(session_id=sid, assistant=assistant_msgs, proposal=convo["proposal"])

@app.post("/freeze", response_model=FreezeResponse)
def freeze(req: FreezeRequest):
    v = next_schema_version()
    now = datetime.utcnow().isoformat() + "Z"
    cmc = dict(req.cmc)
    rpc = dict(req.rpc)
    cmc["schema_version"] = v
    rpc["schema_version"] = v
    try:
        CMC(**cmc)
        RPC(**rpc)
    except ValidationError as e:
        raise HTTPException(
            status_code=400,
            detail={"message": "Invalid CMC/RPC payload", "errors": e.errors()},
        )
    cmc_path = FROZEN_SCHEMA_DIR / f"{v}.json"
    rpc_path = FROZEN_RULEPACK_DIR / f"{v}.json"
    save_json(cmc_path, cmc)
    save_json(rpc_path, rpc)
    hashes = {"cmc": str(cmc_path), "rpc": str(rpc_path)}
    return FreezeResponse(schema_version=v, frozen_at=now, hashes=hashes)

@app.get("/contracts/{schema_version}", response_model=ContractsResponse)
def get_contracts(schema_version: str):
    sp = FROZEN_SCHEMA_DIR / f"{schema_version}.json"
    rp = FROZEN_RULEPACK_DIR / f"{schema_version}.json"
    if not sp.exists() or not rp.exists():
        raise HTTPException(status_code=404, detail="contracts not found")
    return ContractsResponse(cmc=load_json(sp), rpc=load_json(rp))

@app.get("/versions", response_model=VersionListResponse)
def list_versions():
    versions = []
    for p in sorted(FROZEN_SCHEMA_DIR.glob("v*.json")):
        versions.append(p.stem)
    return VersionListResponse(versions=versions)

@app.get("/runs", response_model=RunListResponse)
def list_runs():
    out: List[RunSummary] = []
    if RUNS_DIR.exists():
        for d in RUNS_DIR.iterdir():
            if not d.is_dir():
                continue
            p = d / "manifest.json"
            if not p.exists():
                continue
            try:
                data = load_json(p)
            except Exception:
                continue
            out.append(
                RunSummary(
                    run_id=str(data.get("run_id") or d.name),
                    schema_version=data.get("schema_version"),
                    db_name=data.get("db_name"),
                    sink=data.get("sink"),
                    state=data.get("state"),
                    progress=data.get("progress"),
                    created_at=data.get("created_at"),
                )
            )
    out.sort(key=lambda r: (r.created_at or "", r.run_id), reverse=True)
    return RunListResponse(runs=out)

@app.post("/run", response_model=RunResponse)
async def run(req: RunRequest, background_tasks: BackgroundTasks):
    sp = FROZEN_SCHEMA_DIR / f"{req.schema_version}.json"
    rp = FROZEN_RULEPACK_DIR / f"{req.schema_version}.json"
    if not sp.exists() or not rp.exists():
        raise HTTPException(status_code=404, detail="contracts not found")
    try:
        cmc = CMC(**load_json(sp))
    except ValidationError as e:
        raise HTTPException(
            status_code=400,
            detail={"message": "Frozen CMC is invalid", "errors": e.errors()},
        )
    db_name = f"{req.db_prefix}_{req.schema_version.replace('v','')}"
    ddl = ddl_from_cmc(cmc, db_name)
    sink = (req.sink or "postgres").strip().lower()
    if sink not in ["postgres", "csv"]:
        raise HTTPException(status_code=400, detail="invalid sink; must be postgres or csv")
    run_id = str(uuid4())
    rdir = RUNS_DIR / run_id
    rdir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": run_id,
        "state": "started",
        "progress": 0,
        "db_name": db_name,
        "tables": list(ddl.keys()),
        "ddl": ddl,
        "schema_version": req.schema_version,
        "ddl_approved": False if sink == "postgres" else True,
        "sink": sink,
        "dir": str(rdir),
        "qa_report_url": None,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    RUNS[run_id] = manifest
    save_json(rdir / "manifest.json", manifest)
    background_tasks.add_task(pipeline_run, run_id)
    return RunResponse(run_id=run_id, status="started")

@app.get("/status/{run_id}", response_model=StatusResponse)
def status(run_id: str):
    if run_id not in RUNS:
        p = RUNS_DIR / run_id / "manifest.json"
        if p.exists():
            RUNS[run_id] = load_json(p)
        else:
            raise HTTPException(status_code=404, detail="run not found")
    r = RUNS[run_id]
    return StatusResponse(
        state=r.get("state", "unknown"),
        progress=r.get("progress", 0),
        db_name=r.get("db_name"),
        sink=r.get("sink"),
        tables=r.get("tables", []),
        qa_report_url=r.get("qa_report_url"),
        ddl=r.get("ddl"),
        ddl_approved=r.get("ddl_approved"),
        decision=r.get("decision"),
        decision_reason=r.get("decision_reason"),
        schema_diff=r.get("schema_diff"),
        rows_preview=r.get("rows_preview"),
        inserted=r.get("inserted"),
        csv_dir=r.get("csv_dir"),
        csv_files=r.get("csv_files"),
        exported=r.get("exported"),
    )

@app.post("/approve_ddl/{run_id}", response_model=RunResponse)
async def approve_ddl(run_id: str, background_tasks: BackgroundTasks):
    if run_id not in RUNS:
        p = RUNS_DIR / run_id / "manifest.json"
        if p.exists():
            RUNS[run_id] = load_json(p)
        else:
            raise HTTPException(status_code=404, detail="run not found")
    run = RUNS[run_id]
    run["ddl_approved"] = True
    save_json(Path(run["dir"]) / "manifest.json", run)
    if run.get("state") in ["gate_wait", "emit_ddl", "started"]:
        background_tasks.add_task(pipeline_run, run_id)
    return RunResponse(run_id=run_id, status="approved")
