from typing import Dict, Any, List, Tuple
import json
import os

def _default_proposal() -> Tuple[List[str], Dict[str, Any], Dict[str, Any]]:
    assistant_msgs = ["Acknowledged", "Proposed initial schema and rules"]
    cmc = {
        "schema_version": "v0000",
        "domain_label": "demo",
        "entities": [
            {
                "name": "entity_example",
                "columns": [
                    {"name": "id", "type": "String", "nullable": False},
                    {"name": "created_on", "type": "Date", "nullable": True},
                    {"name": "category", "type": "LowCardinality(String)", "nullable": True},
                ],
            }
        ],
        "relationships": [],
        "nl_sql_hints": {"entity_example": ["item", "record"]},
    }
    rpc = {
        "schema_version": "v0000",
        "rules": [
            {"id": "uniq_id", "type": "dependency", "predicate": "id unique per table", "severity": "error"}
        ],
        "naming": {"tables": "snake_case", "columns": "snake_case"},
        "value_domains": {"category": {"type": "enum", "values": ["alpha", "beta"]}},
    }
    return assistant_msgs, cmc, rpc

def _is_cmc_like(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    if "$schema" in obj or "properties" in obj:
        return False
    if not isinstance(obj.get("schema_version"), str):
        return False
    if not isinstance(obj.get("domain_label"), str):
        return False
    entities = obj.get("entities")
    if not isinstance(entities, list) or len(entities) == 0:
        return False
    for e in entities:
        if not isinstance(e, dict):
            return False
        if not isinstance(e.get("name"), str) or not e.get("name"):
            return False
        cols = e.get("columns")
        if not isinstance(cols, list) or len(cols) == 0:
            return False
    return True

def _is_rpc_like(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    if "$schema" in obj or "properties" in obj:
        return False
    if not isinstance(obj.get("schema_version"), str):
        return False
    rules = obj.get("rules")
    if rules is not None and not isinstance(rules, list):
        return False
    naming = obj.get("naming")
    if naming is not None and not isinstance(naming, dict):
        return False
    value_domains = obj.get("value_domains")
    if value_domains is not None and not isinstance(value_domains, dict):
        return False
    return True

def _extract_json(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except Exception:
                return {}
        return {}

def generate_proposal(message: str, history: List[Dict[str, Any]]) -> Tuple[List[str], Dict[str, Any], Dict[str, Any]]:
    try:
        from crewai import Agent, Task, Crew
    except Exception:
        return _default_proposal()
    verbose_agents = os.getenv("CREW_VERBOSE", "").strip().lower() in {"1", "true", "yes", "y"}
    verbose_crew = os.getenv("CREW_VERBOSE", "").strip().lower() in {"1", "true", "yes", "y"}
    convo = []
    for h in history[-6:]:
        role = h.get("role", "user")
        text = h.get("text", "")
        convo.append(f"{role}: {text}")
    convo_text = "\n".join(convo)
    manager = Agent(
        role="Manager",
        goal="Converge on a concrete schema and rules proposal",
        backstory="Coordinates the design crew and ensures output fidelity",
        allow_delegation=True,
        verbose=verbose_agents,
    )
    schema_designer = Agent(
        role="Schema Designer",
        goal="Propose entities, columns, and relationships",
        backstory="Designs a concise, Postgres-friendly schema",
        allow_delegation=False,
        verbose=verbose_agents,
    )
    constraints_analyst = Agent(
        role="Constraints Analyst",
        goal="Propose constraints and value domains",
        backstory="Defines keys, rules, and value domains",
        allow_delegation=False,
        verbose=verbose_agents,
    )
    t1 = Task(
        description=f"Given the chat, propose entities and columns for a new schema.\nChat:\n{convo_text}\nUser request:\n{message}\nReturn structured notes.",
        expected_output="A concise outline of entities, columns, and relationships.",
        agent=schema_designer,
    )
    t2 = Task(
        description="Define constraints and value domains for the proposed schema. Use snake_case naming.",
        expected_output="A concise outline of rules and value domains.",
        agent=constraints_analyst,
    )
    t4 = Task(
        description="Combine all inputs into a single JSON object with keys cmc and rpc. The output must be JSON only. Do not output JSON Schema (no $schema, no properties). cmc must be an instance of the CMC contract: {\"schema_version\":\"v0000\",\"domain_label\":\"...\",\"entities\":[{\"name\":\"...\",\"columns\":[{\"name\":\"...\",\"type\":\"String|Int|Date|Timestamp|Boolean|Float|Decimal(18,2)\",\"nullable\":true}]}],\"relationships\":[{\"from\":\"child_table\",\"to\":\"parent_table\",\"type\":\"many-to-one\",\"fk\":{\"from_column\":\"child_fk_col\",\"to_column\":\"parent_pk_col\"}}],\"nl_sql_hints\":{}}. rpc must be an instance of the RPC contract: {\"schema_version\":\"v0000\",\"rules\":[],\"naming\":{},\"value_domains\":{}}.",
        expected_output='{"cmc": {...}, "rpc": {...}}',
        agent=manager,
    )
    crew = Crew(agents=[manager, schema_designer, constraints_analyst], tasks=[t1, t2, t4], verbose=verbose_crew)
    output = crew.kickoff(inputs={"message": message, "history": history})
    data = _extract_json(str(output))
    if not data:
        return _default_proposal()
    cmc = data.get("cmc")
    rpc = data.get("rpc")
    if not isinstance(cmc, dict) or not isinstance(rpc, dict):
        return _default_proposal()
    if not _is_cmc_like(cmc) or not _is_rpc_like(rpc):
        return _default_proposal()
    assistant_msgs = ["CrewAI proposal generated", "Review and approve if it matches intent"]
    return assistant_msgs, cmc, rpc
