import streamlit as st
import requests
import os
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from app.config import load_env
load_env()

API_URL = os.getenv("API_URL", "http://localhost:8000")
REQUEST_TIMEOUT_S = float(os.getenv("UI_REQUEST_TIMEOUT_S", "10"))
CHAT_TIMEOUT_S = float(os.getenv("UI_CHAT_TIMEOUT_S", "600"))

st.set_page_config(page_title="Synthetic Data Factory", layout="wide")

st.markdown(
    """
<style>
html, body, [class*="css"]  { font-size: 15px; }
h1 { font-size: 2.1rem; }
h2 { font-size: 1.5rem; }
h3 { font-size: 1.2rem; }

[data-testid="stMetricLabel"] { font-size: 0.9rem; }
[data-testid="stMetricValue"] { font-size: 1.4rem; }

[data-testid="stTabs"] button { font-size: 0.95rem; }
details > summary { font-size: 1.0rem; }

pre, code, [data-testid="stJson"] { font-size: 0.85rem; }
</style>
""",
    unsafe_allow_html=True,
)
if "session_id" not in st.session_state:
    st.session_state.session_id = None
if "proposal" not in st.session_state:
    st.session_state.proposal = None
if "freeze" not in st.session_state:
    st.session_state.freeze = None
if "run_id" not in st.session_state:
    st.session_state.run_id = None
if "messages" not in st.session_state:
    st.session_state.messages = []
if "last_error" not in st.session_state:
    st.session_state.last_error = None
if "selected_schema_version" not in st.session_state:
    st.session_state.selected_schema_version = ""
if "selected_run_id" not in st.session_state:
    st.session_state.selected_run_id = ""
if "sink" not in st.session_state:
    st.session_state.sink = "postgres"

st.title("Synthetic Data Factory")
active_schema_version_main = None
if isinstance(st.session_state.freeze, dict):
    active_schema_version_main = st.session_state.freeze.get("schema_version")
top_a, top_b, top_c, top_d = st.columns([1.2, 1.2, 1.2, 2.4])
top_a.metric("Schema Version", active_schema_version_main or "-")
top_b.metric("Run ID", (st.session_state.run_id or "-")[:8] if st.session_state.run_id else "-")
top_c.metric("Session", (st.session_state.session_id or "-")[:8] if st.session_state.session_id else "-")
top_d.caption(f"API: {API_URL}")

def _safe_get(url: str):
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT_S)
    except Exception as e:
        return None, str(e)
    if not r.ok:
        return None, r.text
    try:
        return r.json(), None
    except Exception:
        return None, r.text

def _safe_post(url: str, body: dict):
    try:
        r = requests.post(url, json=body, timeout=REQUEST_TIMEOUT_S)
    except Exception as e:
        return None, str(e)
    if not r.ok:
        return None, r.text
    try:
        return r.json(), None
    except Exception:
        return None, r.text

versions_payload, _ = _safe_get(f"{API_URL}/versions")
available_versions = (versions_payload or {}).get("versions", [])
if not isinstance(available_versions, list):
    available_versions = []

runs_payload, _ = _safe_get(f"{API_URL}/runs")
available_runs = (runs_payload or {}).get("runs", [])
if not isinstance(available_runs, list):
    available_runs = []

def _run_label(r: dict) -> str:
    rid = str(r.get("run_id", ""))
    sv = str(r.get("schema_version", ""))
    stt = str(r.get("state", ""))
    prog = r.get("progress")
    prog_s = f"{prog}%" if isinstance(prog, int) else ""
    return " ".join([x for x in [rid[:8], sv, stt, prog_s] if x])

def _reset_context() -> None:
    st.session_state.session_id = None
    st.session_state.proposal = None
    st.session_state.freeze = None
    st.session_state.run_id = None
    st.session_state.messages = []
    st.session_state.last_error = None
    st.session_state.selected_schema_version = ""
    st.session_state.selected_run_id = ""
    st.session_state.sink = "postgres"

with st.sidebar:
    st.subheader("Active Context")
    active_schema_version = None
    if isinstance(st.session_state.freeze, dict):
        active_schema_version = st.session_state.freeze.get("schema_version")
    st.write(
        {
            "api_url": API_URL,
            "session_id": st.session_state.session_id,
            "schema_version": active_schema_version,
            "run_id": st.session_state.run_id,
        }
    )
    if st.button("Reset Session"):
        _reset_context()
        st.rerun()

    st.subheader("Load Existing")
    st.selectbox(
        "Frozen schema version",
        options=[""] + available_versions,
        key="selected_schema_version",
    )
    if st.button("Load Contracts"):
        sv = st.session_state.selected_schema_version
        if sv:
            data, err = _safe_get(f"{API_URL}/contracts/{sv}")
            if err:
                st.session_state.last_error = err
            else:
                st.session_state.proposal = {"cmc": data.get("cmc"), "rpc": data.get("rpc")}
                st.session_state.freeze = {"schema_version": sv}
                st.session_state.messages.append({"role": "assistant", "content": f"Loaded frozen contracts: {sv}"})

    run_options = [""] + [str(r.get("run_id")) for r in available_runs if isinstance(r, dict) and r.get("run_id")]
    run_meta = {str(r.get("run_id")): r for r in available_runs if isinstance(r, dict) and r.get("run_id")}
    st.selectbox(
        "Existing run",
        options=run_options,
        format_func=lambda rid: "" if not rid else _run_label(run_meta.get(rid, {"run_id": rid})),
        key="selected_run_id",
    )
    if st.button("Load Run"):
        rid = st.session_state.selected_run_id
        if rid:
            st.session_state.run_id = rid
            meta = run_meta.get(rid, {})
            sv = meta.get("schema_version")
            if isinstance(sv, str) and sv:
                st.session_state.freeze = {"schema_version": sv}
            sink = meta.get("sink")
            if isinstance(sink, str) and sink:
                st.session_state.sink = sink
            st.session_state.messages.append({"role": "assistant", "content": f"Loaded run: {rid}"})

def _send_to_crew(msg: str) -> None:
    if not msg.strip():
        return
    st.session_state.last_error = None
    st.session_state.messages.append({"role": "user", "content": msg})
    payload = {"session_id": st.session_state.session_id, "message": msg}
    try:
        r = requests.post(f"{API_URL}/chat", json=payload, timeout=max(REQUEST_TIMEOUT_S, CHAT_TIMEOUT_S))
    except Exception as e:
        st.session_state.last_error = str(e)
        return
    if not r.ok:
        st.session_state.last_error = r.text
        return
    data = r.json()
    st.session_state.session_id = data["session_id"]
    st.session_state.proposal = data.get("proposal")
    assistant_msgs = data.get("assistant") or []
    if isinstance(assistant_msgs, list):
        for t in assistant_msgs:
            st.session_state.messages.append({"role": "assistant", "content": str(t)})

if st.session_state.last_error:
    st.error(st.session_state.last_error)

tab_chat, tab_review, tab_run = st.tabs(["Chat", "Human Review", "Run"])

with tab_chat:
    st.subheader("Conversation")
    for m in st.session_state.messages:
        with st.chat_message(m["role"]):
            st.write(m["content"])
    prompt = st.chat_input("Ask for a schema or rules (fresh session works without selecting anything)...")
    if prompt:
        with st.spinner("Generating proposal..."):
            _send_to_crew(prompt)
        st.rerun()

with tab_review:
    st.subheader("Proposal Review")
    if not st.session_state.proposal:
        st.info("No proposal yet. Use the Chat tab to generate one, or load frozen contracts from the sidebar.")
    else:
        cmc = st.session_state.proposal.get("cmc") if isinstance(st.session_state.proposal, dict) else None
        rpc = st.session_state.proposal.get("rpc") if isinstance(st.session_state.proposal, dict) else None

        left, right = st.columns(2)
        with left:
            with st.expander("CMC (Canonical Model Contract)", expanded=True):
                if isinstance(cmc, dict):
                    st.json(cmc, expanded=False)
                else:
                    st.write(cmc)
        with right:
            with st.expander("RPC (Rule Pack Contract)", expanded=True):
                if isinstance(rpc, dict):
                    st.json(rpc, expanded=False)
                else:
                    st.write(rpc)

        a, b = st.columns([1, 3])
        if a.button("Freeze Contracts", type="primary"):
            body = {
                "session_id": st.session_state.session_id,
                "cmc": cmc,
                "rpc": rpc,
            }
            data, err = _safe_post(f"{API_URL}/freeze", body)
            if err:
                st.session_state.last_error = err
            else:
                st.session_state.freeze = data
                st.session_state.messages.append({"role": "assistant", "content": f"Contracts frozen: {data.get('schema_version')}"})
                st.rerun()
        if b.button("Clear Proposal"):
            st.session_state.proposal = None
            st.session_state.freeze = None
            st.rerun()

        if isinstance(st.session_state.freeze, dict) and st.session_state.freeze.get("schema_version"):
            st.success(f"Frozen schema_version: {st.session_state.freeze.get('schema_version')}")

with tab_run:
    st.subheader("Build and Approvals")
    if not (isinstance(st.session_state.freeze, dict) and st.session_state.freeze.get("schema_version")):
        st.info("Freeze contracts first (Human Review tab) or load a schema version from the sidebar.")
    else:
        schema_version = st.session_state.freeze["schema_version"]
        target_schema = f"synthetic_{schema_version.replace('v','')}"
        top_run = st.columns([1.1, 1.1, 2.8])
        top_run[0].selectbox("Sink", options=["postgres", "csv"], key="sink")
        top_run[1].write({"target_schema": target_schema})
        if st.session_state.sink == "csv":
            top_run[2].info("CSV sink skips Postgres insertion and DDL approval. Artifacts are written to artifacts/runs/<run_id>/csv/*.csv")
        else:
            top_run[2].info("Postgres sink inserts into Postgres (requires DDL approval) and also exports CSV artifacts.")

        run_cols = st.columns([1, 3])
        if run_cols[0].button("Run Build", type="primary"):
            body = {"schema_version": schema_version, "sink": st.session_state.sink}
            data, err = _safe_post(f"{API_URL}/run", body)
            if err:
                st.session_state.last_error = err
            else:
                st.session_state.run_id = data.get("run_id")
                st.rerun()

        if st.session_state.run_id:
            status, err = _safe_get(f"{API_URL}/status/{st.session_state.run_id}")
            if err:
                st.session_state.last_error = err
            else:
                with st.expander("Run Status", expanded=True):
                    st.json(status, expanded=False)
                if status.get("schema_diff"):
                    with st.expander("Schema Diff Decision", expanded=False):
                        st.json(status.get("schema_diff"), expanded=False)
                        st.write(status.get("decision"))
                        st.write(status.get("decision_reason"))
                if status.get("ddl") and status.get("sink") == "postgres" and not status.get("ddl_approved"):
                    with st.expander("DDL Preview", expanded=True):
                        st.json(status.get("ddl"), expanded=False)
                    if st.button("Approve DDL", type="primary"):
                        data, err = _safe_post(f"{API_URL}/approve_ddl/{st.session_state.run_id}", {})
                        if err:
                            st.session_state.last_error = err
                        else:
                            st.session_state.run_id = data.get("run_id")
                            st.rerun()
                if status.get("csv_dir") or status.get("csv_files"):
                    with st.expander("CSV Export", expanded=True):
                        if status.get("exported"):
                            st.write({"exported_rows": status.get("exported")})
                        if status.get("csv_dir"):
                            st.write({"csv_dir": status.get("csv_dir")})
                        if isinstance(status.get("csv_files"), dict):
                            st.write({"csv_files": status.get("csv_files")})
