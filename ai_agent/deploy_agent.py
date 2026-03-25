"""
Deploy MedAtlasAgent to Databricks Model Serving.

Usage:
  1. Create UC function first (if not already):
       uv run python agent/run_sql.py agent/setup_uc_function.sql

  2. Run this script:
       uv run python agent/deploy_agent.py

Steps:
  - Register the agent as an MLflow pyfunc model in Unity Catalog
  - Deploy to a Databricks serving endpoint via agents.deploy()
"""

import os
from pathlib import Path
from dotenv import load_dotenv
# Import agent — sets model via set_model(AGENT) at bottom of agent.py
from .agent import AGENT, LLM_ENDPOINT


_dot = Path(__file__).parent.parent / ".env"
load_dotenv(_dot)

import mlflow
from mlflow.models.resources import DatabricksServingEndpoint
from mlflow.models.signature import ModelSignature
from mlflow.types import ColSpec, DataType, Schema

CATALOG = os.environ.get("CATALOG", "med_atlas_ai")
REGISTERED_NAME = f"{CATALOG}.agent.med_atlas_agent"
ENDPOINT_NAME = "med-atlas-agent-endpoint"

mlflow.set_registry_uri("databricks-uc")

# ── Patch MLflow validator to allow dots in UC model names ────────────────────
import mlflow.utils.validation as _mlflow_val
_orig_validate = _mlflow_val._validate_logged_model_name

def _patched_validate(name):
    if name and "." in name:
        return  # UC names are catalog.schema.model — dots are required
    _orig_validate(name)

_mlflow_val._validate_logged_model_name = _patched_validate

# ── Register model ──────────────────────────────────────────────────────────
# ResponsesAgent inherits from PythonModel — use pyfunc flavor
signature = ModelSignature(
    inputs=Schema([ColSpec(DataType.string, "input")]),
    outputs=Schema([ColSpec(DataType.string, "output")]),
)

resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
]

print(f"Logging agent to UC: {REGISTERED_NAME}")
with mlflow.start_run(run_name="med-atlas-agent-deploy"):
    model_info = mlflow.pyfunc.log_model(
        python_model=AGENT,
        artifact_path="med_atlas_agent",
        signature=signature,
        resources=resources,
        pip_requirements=[
            "mlflow-skinny>=2.11.3",
            "databricks-langchain>=0.1.0",
            "langchain-core>=0.2.0",
            "langgraph>=0.3.0",
        ],
        registered_model_name=REGISTERED_NAME,
    )

print(f"Model registered: {REGISTERED_NAME} v{model_info.registered_model_version}")

# ── Deploy ────────────────────────────────────────────────────────────────
from databricks import agents

print(f"\nDeploying to endpoint: {ENDPOINT_NAME}")
deployment = agents.deploy(
    REGISTERED_NAME,
    model_version=int(model_info.registered_model_version),
    endpoint_name=ENDPOINT_NAME,
    tags={"source": "mcp", "team": "med-atlas"},
)

print(f"\nDeployment initiated!")
print(f"  Endpoint: {deployment.endpoint_name}")
print(f"  Query:    {deployment.query_endpoint}")
print(f"  Review:   {deployment.endpoint_url}")
print(f"\nDeployment takes ~15 minutes. Check status:")
print(f"  uv run python -c \"from databricks.agents import get_deployments; "
      f"print(get_deployments('{REGISTERED_NAME}'))\"")
