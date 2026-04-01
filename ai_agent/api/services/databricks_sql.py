import os
import json
from databricks.sdk import WorkspaceClient

def execute_sql(query: str) -> list[dict]:
    """
    Executes a SQL statement via the Databricks Databricks Statement Execution API.
    Returns a list of dictionaries mapping column names to values.
    """
    w = WorkspaceClient()
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        raise ValueError("Environment variable DATABRICKS_WAREHOUSE_ID is required for SQL execution.")

    catalog = os.environ.get("CATALOG", "med_atlas_ai")
    schema = os.environ.get("SCHEMA", "default")

    response = w.statement_execution.execute_statement(
        statement=query,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        wait_timeout="30s"
    )

    if not response.result or not response.result.data_array:
        return []

    # Map the data array to dictionaries using the schema columns
    columns = [col.name for col in response.manifest.schema.columns]
    results = []
    
    for row in response.result.data_array:
        row_dict = {}
        for idx, col_name in enumerate(columns):
            val = row[idx]
            # Handle JSON strings for arrays if returned as string by Databricks SDK
            if isinstance(val, str) and (val.startswith('[') and val.endswith(']')):
                try:
                    val = json.loads(val)
                except Exception:
                    pass
            row_dict[col_name] = val
        results.append(row_dict)

    return results
