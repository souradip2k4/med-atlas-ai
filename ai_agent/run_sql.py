"""
Execute SQL on Databricks using the Python SDK.
Runs setup_uc_function.sql and the test queries.
"""
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

HOST = os.environ["DATABRICKS_HOST"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
cfg = Config(host=HOST, token=TOKEN)

w = WorkspaceClient(config=cfg)
sql = w.statement_execution


def _split_statements(content: str) -> list[str]:
    """Split on semicolons, handling string literals minimally."""
    stmts = []
    buf = []
    in_str = False
    str_char = None
    for ch in content:
        if ch in ("'", '"') and not in_str:
            in_str = True
            str_char = ch
        elif ch == str_char and in_str:
            in_str = False
            str_char = None
        elif ch == ";" and not in_str:
            if buf:
                stmts.append("".join(buf))
                buf = []
            continue
        buf.append(ch)
    if buf:
        stmts.append("".join(buf))
    return stmts



def execute_file(sql_file: Path) -> list[dict]:
    """Execute a .sql file (multi-statement) via theExecute File API."""
    content = sql_file.read_text()
    # Split on semicolons that end statements (skip comments)
    results = []
    for stmt in _split_statements(content):
        stmt = stmt.strip()
        if not stmt or stmt.startswith("--"):
            continue
        print(f"\nExecuting: {stmt[:80]}...")
        try:
            q = sql.execute_statement(
                statement=stmt,
                warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
                catalog=os.environ.get("CATALOG", "med_atlas_ai"),
                schema=os.environ.get("SCHEMA", "default"),
            )
            if q.result:
                for row in q.result.data_array:
                    print("  ", row)
            print(f"  ✓ OK — {q.status}")
        except Exception as exc:
            print(f"  ✗ ERROR: {exc}")
            results.append({"statement": stmt[:60], "error": str(exc)})
    return results

if __name__ == "__main__":
    import sys
    sql_file = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "setup_uc_function.sql"
    print(f"Running {sql_file} on {HOST}")
    errors = execute_file(sql_file)
    if errors:
        print(f"\n{len(errors)} statement(s) had errors:")
        for e in errors:
            print(f"  - {e}")
    else:
        print("\n✓ All statements executed successfully")
