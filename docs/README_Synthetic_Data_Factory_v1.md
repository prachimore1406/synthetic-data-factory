# Synthetic Data Factory – Conversational Schema Design + Deterministic Generation  
**Final Requirements & Architecture v1**

**Stack (frozen):**  
- **Streamlit** – Conversational UI  
- **FastAPI** – Backend & orchestration API  
- **CrewAI** – Conversational, role‑based schema/rule design crew  
- **LangGraph** – Workflow orchestration; GenAI used only for schema‑diff decisions  
- **PostgreSQL** – Relational DB with schema‑versioned namespaces

**Versioning policy:** **Every approved build creates a brand‑new Postgres schema** (e.g., `synthetic_0008`). Old versions remain immutable for lineage, reproducibility, and dataset rollbacks.

---

## 1) Overview

This project delivers a **schema/domain‑agnostic synthetic‑data factory**:

- Users **chat** with a **CrewAI “design crew”** (Manager, Schema Designer, Constraints Analyst) to ideate schemas and rules.  
- The UI separates work into **Chat → Human Review → Run** tabs to keep generation, contract approval, and execution clearly staged.  
- On **human approval**, the proposal is **frozen** into two JSON contracts:  
  - **CMC** – Canonical Model Contract (entities/columns/relationships/hints)  
  - **RPC** – Rule Pack Contract (constraints/value domains/naming rules)  
- In a **fresh UI session**, users can **load an existing schema version or run** to continue work against a specific frozen version and its target Postgres schema.  
- A **LangGraph** pipeline **deterministically** transforms those contracts into:  
  - **Postgres DDL** (creating a new schema `synthetic_00NN`)  
  - **Synthetic row generation** (rule-aware, deterministic)  
  - **Optional bulk inserts** into Postgres (controlled by `/run.sink`) and a **run manifest**
  - **CSV exports** written to `artifacts/runs/<run_id>/csv/<table>.csv`
  - **QA validation report** (rule + value-domain + referential integrity checks)
  - GenAI is only used in the **schema‑diff decision** node

---

## 2) High‑Level Architecture (Block Diagram)

```
┌─────────────────────┐     ┌──────────────────────┐     ┌──────────────────────────────┐
│   Streamlit UI      │ --> │    FastAPI API        │ --> │ CrewAI Design Crew           │
│ chat / preview /    │     │ /chat /freeze /run    │     │ Manager, Designer, Analyst   │
│ approve / status    │     │ /status /approve_ddl  │                                  │
└─────────────────────┘     └──────────────────────┘     └──────────────────────────────┘
              │                          │
              │                          ▼
              │                 ┌──────────────────────────────┐
              │                 │ LangGraph Agentic Workflow   │
              │                 │ Validate → Emit DDL → Decide │
              │                 │ → Human Gate → Generate      │
              │                 │ → (Insert?) → Export CSV → QA │
              │                 └──────────────────────────────┘
              │                          │
              │                          ▼
              │                 ┌──────────────────────────────┐
              │                 │ Postgres (schema per run)    │
              │                 │ synthetic_0008, synthetic_0007│
              │                 └──────────────────────────────┘
              │
              ▼
     ┌──────────────────────────┐
     │ Artifacts                │
     │ Frozen CMC/RPC, manifest │
     │ QA report                │
     └──────────────────────────┘
```

---

## 3) Swimlane Diagram (Block Diagram)

```
User
  Start Chat or Load Version/Run → Approve Schema → Approve DDL → Watch Run Status

Streamlit UI
  Render chat & proposal → Freeze contracts → Trigger /run → Poll /status (or Load /versions, /runs)

FastAPI
  /chat → /freeze → /run → /status (plus /versions, /runs)

CrewAI
  Role-based ideation (Manager, Designer, Analyst)

LangGraph
-  Validate → Emit DDL → Schema Diff Decision → Human Gate → Generate → (Insert?) → Export CSV → QA

Postgres
  Create Schema → Create Tables → Insert Data
```

---

## 4) Agents & Tools

### 4.1 CrewAI – “Design Crew”

**Roles**

- **Manager** – coordinates the conversation and convergence  
- **Schema Designer** – proposes entities, columns, types, relationships  
- **Constraints Analyst** – proposes rules, keys, CHECKs, naming standards  

**Tools used by CrewAI**

- **JSON Contract Emitter** – produces strict **CMC** & **RPC** JSON (no prose)  
- **Contract Linter** – validates SQL types, naming rules, required fields  
- **Glossary & Synonyms Generator** – fills `nl_sql_hints` for NL‑to‑SQL consumers  
- **Change Impact & Diff Helper** – shows deltas vs. last frozen version

### 4.2 LangGraph – Agentic Build Pipeline

**Nodes / tools**

- **Contract Validator Node** – coherence checks for CMC & RPC  
- **DDL Emitter Node** – generates `CREATE SCHEMA synthetic_000N` and `CREATE TABLE` per entity  
- **Schema Diff Decision Node** – GenAI decision to choose append/block  
- **Human Gate Node** – pauses until DDL is approved in the UI  
- **Deterministic Data Generator Node** – rule-aware generation (not LLM)  
- **Postgres Connector Node** – uses `psycopg2` for DDL + optional bulk inserts  
- **CSV Export Node** – writes `artifacts/runs/<run_id>/csv/<table>.csv`  
- **QA Validator Node** – deterministic checks for rule predicates, value domains, and referential integrity  
- **Manifest Writer Node** – persists run metadata (schema_version, hashes, DDL, QA)

---

## 5) Contracts (Schema‑Agnostic)

### 5.1 Canonical Model Contract (CMC)

```json
{
  "schema_version": "v0008",
  "domain_label": "string",
  "entities": [
    {
      "name": "string",
      "columns": [
        { "name": "string", "type": "PostgresType", "nullable": true, "semantic": "optional note" }
      ]
    }
  ],
  "relationships": [
    {
      "from": "child_table",
      "to": "parent_table",
      "type": "one-to-many|many-to-one|many-to-many",
      "fk": { "from_column": "child_fk_col", "to_column": "parent_pk_col" }
    }
  ],
  "nl_sql_hints": {
    "entity_or_column": ["synonym", "alias"]
  }
}
```

`relationships[*].fk` can be either an object (`{"from_column": "...", "to_column": "..."}`) or a string (`"child_col->parent_table.parent_col"`).

### 5.2 Rule Pack Contract (RPC)

```json
{
  "schema_version": "v0008",
  "rules": [
    {
      "id": "string",
      "type": "equals|prefix|temporal|dependency|value_domain|custom",
      "predicate": "free-form expression readable by validators",
      "severity": "warning|error"
    }
  ],
  "generation": { "row_count": 50 },
  "naming": { "tables": "snake_case", "columns": "snake_case" },
  "value_domains": {
    "column_name": {
      "type": "enum|range|regex",
      "values": ["A", "B"]
    }
  }
}
```

---

## 6) API (FastAPI)

```http
POST /chat
  body: { session_id?, message }
  resp: { session_id, assistant[], proposal?: { cmc, rpc } }

POST /freeze
  body: { session_id, cmc, rpc }
  resp: { schema_version: "v0008", frozen_at, hashes }

POST /run
  body: { schema_version: "v0008", db_prefix?: "synthetic", sink?: "postgres|csv" }
  resp: { run_id, status: "started" }

GET /status/{run_id}
  resp: { state, progress, db_name, sink, tables, ddl, ddl_approved, decision, decision_reason, schema_diff, qa_report_url, rows_preview, inserted?, csv_dir, csv_files, exported }

POST /approve_ddl/{run_id}
  resp: { run_id, status }

GET /contracts/{schema_version}
  resp: { cmc, rpc }

GET /versions
  resp: { versions: ["v0001", "v0002", ...] }

GET /runs
  resp: { runs: [{ run_id, schema_version, db_name, sink, state, progress, created_at? }, ...] }
```

**Typical flow**

1. `POST /chat` iterates design with CrewAI.  
2. `POST /freeze` freezes contracts, assigns next `schema_version`.  
3. `POST /run` triggers LangGraph to build `synthetic_00NN`.  
4. `GET /status/{run_id}` polls progress (Validate → DDL → Decide → Gate → Generate → (Insert?) → Export CSV → QA).

**Fresh-session continuation**

- Load a frozen contract via `GET /versions` → `GET /contracts/{schema_version}` and run it again, or
- Load an existing run via `GET /runs` → `GET /status/{run_id}` to resume monitoring and approvals.

---

## 7) Postgres Versioning – New Schema per Run

**Examples**

```
synthetic_0001
synthetic_0002
synthetic_0003
...
```

**DDL (illustrative)**

```sql
CREATE SCHEMA IF NOT EXISTS "synthetic_0008";

CREATE TABLE IF NOT EXISTS "synthetic_0008"."entity_example" (
  id text,
  created_on date,
  category text
);
```

**Insert from Python (pandas)**

```python
import psycopg2

conn = psycopg2.connect(host=HOST, port=5432, user=USER, password=PWD, dbname=DB)
conn.autocommit = True
cur = conn.cursor()
cur.execute('CREATE SCHEMA IF NOT EXISTS "synthetic_0008"')
cur.execute("""
CREATE TABLE IF NOT EXISTS "synthetic_0008"."entity_example" (
  id text,
  created_on date,
  category text
)
""")
cur.execute(
  'INSERT INTO "synthetic_0008"."entity_example" (id, created_on, category) VALUES (%s, %s, %s)',
  ("A1", "2026-02-23", "alpha"),
)
```

---

## 8) Setup & Run

### 8.1 Prerequisites
- Python 3.11+  
- Docker (for local Postgres)

### 8.2 Create `.env`

```bash
# FastAPI
API_PORT=8000
API_KEY=local-dev-key

# LLM key for CrewAI (OpenAI)
OPENAI_API_KEY=sk-...
OPENAI_BASE_URL=
OPENAI_ORG_ID=
OPENAI_MODEL=

# Postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=
POSTGRES_DB=postgres
```

The application loads `.env` automatically via `app/config.py`.

### 8.3 Install & launch

```bash
# 1) Install backend + UI deps
pip install -r requirements.txt

# 2) Start Postgres
docker run -d --name pg -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres

# 3) Start FastAPI (tab 1)
uvicorn app.api.main:app --reload --port 8000

# 4) Start Streamlit (tab 2)
streamlit run app/ui/streamlit_app.py
```

---

## 9) Directory Layout (suggested)

```
/app
  /ui                 # Streamlit
    streamlit_app.py
    components/*.py
  /api                # FastAPI
    main.py
    routes/*.py
  /crewai             # Crew definitions & prompts
    crew.py
    prompts/*.md
  /graph              # LangGraph workflow
    graph.py
    nodes/*.py
  /db
    postgres_io.py
  rules.py
  relationships.py
  config.py
  /artifacts
    frozen/
      schema/v0008.json
      rulepack/v0008.json
    runs/
      <run_id>/manifest.json
      <run_id>/rows.json
      <run_id>/csv/<table>.csv
      <run_id>/qa_report.json
  requirements.txt
  .env
  README.md
```

---

## 10) Non‑Functional Requirements

- **Determinism & Replayability** – same contracts + params ⇒ same DDL & rows; human gate before DDL ensures safety.  
- **Performance at scale** – millions of rows per run; vectorized generation (NumPy/Polars) means row creation is CPU‑bound, not token‑bound.  
- **Observability** – run manifest with schema_version, hashes, emitted DDL, QA; logs; optional tracing.  
- **Security** – API key in dev; upgrade to OAuth/Entra later.

**UI quality-of-life**

- JSON review is rendered as an expandable tree (CMC/RPC) to support drill-down while reviewing large contracts.
- Request timeouts are configurable via `UI_REQUEST_TIMEOUT_S` and `UI_CHAT_TIMEOUT_S`.

---

## 10.1 Current Implementation Notes

- Inserts are executed via bulk insert; the inserted row counts are returned in run status.  
- QA validates RPC rules/value domains and CMC relationships for referential integrity.  

---

## 11) Notes & Best Practices

- **Single orchestrator**: Keep LangGraph as the only orchestrator. CrewAI designs; LangGraph builds.  
- **No mutation**: Never UPDATE/DELETE past data. Each run = new schema (`synthetic_00NN`).  
- **QA gates**: Add domain validators early; fail fast with actionable messages.  
- **Cost control**: LLM usage confined to chat & proposal JSON and schema‑diff decisions; row generation uses code, not tokens.

---

## 12) Roadmap (optional)

- **Docker Compose** for UI + API + Postgres  
- **Tracing** (e.g., LangSmith) for pipeline introspection  
- **A2A/MCP** exposure to integrate with Azure AI Foundry  
- **Auth**: switch API key → OAuth, add RBAC for approvals
- **Data distribution control**: trends/seasonality, skewed popularity (Zipf), correlations, basket behavior, and other realistic generators driven by an RPC distribution spec

---

## 13) License & Security

- Prototype for internal experimentation.  
- Review prompts/outputs for governance before production.  
- Keep LLM keys and DB creds out of source control.
