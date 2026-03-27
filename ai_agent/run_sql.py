"""
Execute SQL on Databricks using the Python SDK.
Runs setup_uc_function.sql and the test queries.
"""
import os
import re
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from dotenv import load_dotenv

_env_path = Path(__file__).parent.parent / ".env"
load_dotenv(_env_path)


HOST = os.environ["DATABRICKS_HOST"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
cfg = Config(host=HOST, token=TOKEN)

w = WorkspaceClient(config=cfg)
sql = w.statement_execution


def _split_statements(content: str) -> list[str]:
    """
    Split SQL content into executable statements.

    Handles CREATE OR REPLACE FUNCTION blocks specially — the entire function
    definition (RETURNS ... END) is collected as one statement even though it
    contains internal semicolons inside CASE/WHEN branches.

    Everything else is split on semicolons.
    """
    stmts = []
    buf = []
    in_str = False
    str_char = None
    i = 0
    n = len(content)

    while i < n:
        ch = content[i]

        # Track string literals (single-quoted only — SQL uses ' for strings)
        if ch == "'" and not in_str:
            in_str = True
            str_char = "'"
            buf.append(ch)
            i += 1
            continue
        elif ch == str_char and in_str:
            # Check for escaped quote ('') inside a string
            if i + 1 < n and content[i + 1] == "'":
                # '' is an escaped quote, not the end of the string
                buf.append(ch)
                buf.append(content[i + 1])
                i += 2
                continue
            in_str = False
            str_char = None
            buf.append(ch)
            i += 1
            continue

        if in_str:
            buf.append(ch)
            i += 1
            continue

        # Check for CREATE OR REPLACE FUNCTION — collect until final END;
        if ch == "C" and content[i:i + 29] == "CREATE OR REPLACE FUNCTION":
            # Collect everything from here until the final END followed by );
            func_buf = []
            # Advance past "CREATE OR REPLACE FUNCTION"
            remaining = content[i:]
            func_end_match = re.search(r'\bEND\b\s*;', remaining)
            if func_end_match:
                end_pos = func_end_match.end()
                func_body = remaining[:end_pos]
                # lstrip: remove leading whitespace (comments) but preserve trailing END;
                stmts.append(func_body.lstrip())
                i += end_pos
                continue
            # Fallback: collect to end of content
            stmts.append(remaining.rstrip())
            break

        if ch == ";":
            line = "".join(buf).strip()
            if line:
                stmts.append(line)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    if buf:
        line = "".join(buf).strip()
        if line:
            stmts.append(line)

    return stmts


def execute_file(sql_file: Path) -> list[dict]:
    """Execute a .sql file (multi-statement) via the Statement Execution API."""
    content = sql_file.read_text()
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
            print(f"  OK — {q.status}")
        except Exception as exc:
            print(f"  ERROR: {exc}")
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
        print("\nAll statements executed successfully")
