from typing import Dict, Any, List
from ...db.postgres_io import try_connect, execute_ddl
import os

def bulk_insert(context: Dict[str, Any], rows: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    username = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    database = os.getenv("POSTGRES_DB", "postgres")
    client = try_connect(host, port, username, password, database)
    ddl = context.get("ddl", {})
    db_name = context["db_name"]
    execute_ddl(client, ddl, db_name)
    inserted: Dict[str, int] = {}
    if client is None:
        return inserted
    if not isinstance(rows, dict):
        return inserted
    try:
        from psycopg2.extras import execute_values
    except Exception:
        return inserted
    cmc = context.get("cmc", {})
    cmc_entities = {e.get("name"): e for e in cmc.get("entities", []) if isinstance(e, dict)}
    with client.cursor() as cur:
        for table, data in rows.items():
            if not isinstance(table, str) or not isinstance(data, list) or not data:
                continue
            entity = cmc_entities.get(table, {})
            cols = entity.get("columns", [])
            if isinstance(cols, list) and cols:
                col_names = [c.get("name") for c in cols if isinstance(c, dict) and isinstance(c.get("name"), str)]
            else:
                col_names = list(data[0].keys())
            if not col_names:
                continue
            values = [tuple(row.get(c) for c in col_names) for row in data if isinstance(row, dict)]
            if not values:
                continue
            cols_sql = ", ".join([f'"{c}"' for c in col_names])
            sql = f'INSERT INTO "{db_name}"."{table}" ({cols_sql}) VALUES %s'
            execute_values(cur, sql, values, page_size=1000)
            inserted[table] = len(values)
    return inserted
