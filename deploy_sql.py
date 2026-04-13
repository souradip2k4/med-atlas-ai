"""
deploy_sql.py — Execute a SQL file against Databricks SQL Warehouse.

Usage:
    uv run python deploy_sql.py ai_agent/setup_uc_function.sql
    uv run python deploy_sql.py ai_agent/setup_geospatial.sql
"""

import sys
import os
import time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from dotenv import load_dotenv

load_dotenv()

WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

def execute_sql_file(sql_file: str) -> None:
    sql = Path(sql_file).read_text()
    
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    print(f"\n{'='*60}")
    print(f"Deploying: {sql_file}")
    print(f"Warehouse: {WAREHOUSE_ID}")
    print(f"SQL length: {len(sql)} chars")
    print(f"{'='*60}\n")

    response = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="0s",  # async
    )
    stmt_id = response.statement_id
    print(f"Statement submitted: {stmt_id}")

    # Poll until done
    while True:
        status = w.statement_execution.get_statement(stmt_id)
        state = status.status.state
        print(f"  [{time.strftime('%H:%M:%S')}] State: {state.value}")
        if state in (StatementState.SUCCEEDED, StatementState.FAILED,
                     StatementState.CANCELED, StatementState.CLOSED):
            break
        time.sleep(3)

    if state == StatementState.SUCCEEDED:
        print(f"\n✅ SUCCESS: {sql_file} deployed!\n")
    else:
        err = status.status.error
        print(f"\n❌ FAILED: {sql_file}")
        print(f"   Error code: {err.error_code if err else 'N/A'}")
        print(f"   Message:    {err.message if err else 'N/A'}\n")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: uv run python deploy_sql.py <sql_file>")
        sys.exit(1)
    execute_sql_file(sys.argv[1])
