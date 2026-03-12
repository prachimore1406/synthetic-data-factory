from typing import Dict, List, Optional

def try_connect(host: str, port: int, username: str, password: str, database: str):
    try:
        import psycopg2
    except Exception:
        return None
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            dbname=database,
        )
        conn.autocommit = True
        return conn
    except Exception:
        return None

def execute_ddl(client, ddl_map: Dict[str, str], schema_name: str):
    if client is None:
        return False
    try:
        with client.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
            for _, sql in ddl_map.items():
                cur.execute(sql)
        return True
    except Exception:
        return False

def list_tables(client, schema_name: str) -> List[str]:
    if client is None:
        return []
    try:
        with client.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
                (schema_name,),
            )
            rows = cur.fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []

def get_table_columns(client, schema_name: str, table: str) -> Dict[str, str]:
    if client is None:
        return {}
    try:
        with client.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema_name, table),
            )
            rows = cur.fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}
