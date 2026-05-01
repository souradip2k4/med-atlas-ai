import os
from pathlib import Path
from dotenv import load_dotenv
import mlflow
import warnings
_env_path = Path(__file__).parent.parent / ".env"
load_dotenv(_env_path)
# ─── Configuration ─────────────────────────────────────────────────────────────

LLM_ENDPOINT  = os.environ["LLM_ENDPOINT"]
VS_INDEX     = os.environ.get("VECTOR_SEARCH_INDEX")
GENIE_ID     = os.environ["GENIE_SPACE_ID"]
CATALOG      = os.environ.get("CATALOG")
SCHEMA       = os.environ.get("SCHEMA", "default")

# Prefer resource-injected UC function names (Databricks Apps), with fallback for local/dev.
ANALYZE_UC_FUNCTION_NAME = os.environ.get("ANALYZE_UC_FUNCTION_NAME")
if not ANALYZE_UC_FUNCTION_NAME and CATALOG and SCHEMA:
    ANALYZE_UC_FUNCTION_NAME = f"{CATALOG}.{SCHEMA}.analyze_medical_query"

GEOSPATIAL_UC_FUNCTION_NAME = os.environ.get("GEOSPATIAL_UC_FUNCTION_NAME")
if not GEOSPATIAL_UC_FUNCTION_NAME and CATALOG and SCHEMA:
    GEOSPATIAL_UC_FUNCTION_NAME = f"{CATALOG}.{SCHEMA}.find_facilities_nearby"



# experiment_id = os.getenv("MLFLOW_EXPERIMENT_ID")
# tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
# registry_uri = os.getenv("MLFLOW_REGISTRY_URI")
# Optional overrides:
# - In Databricks Apps, experiment resource injection is enough for tracking.
# - For local remote tracking, you can still set MLFLOW_TRACKING_URI / auth env vars.
# if tracking_uri:
#     mlflow.set_tracking_uri(tracking_uri)

# # Registry URI selection:
# # - Honor explicit MLFLOW_REGISTRY_URI.
# # - Default to Unity Catalog registry when using Databricks tracking.
# if registry_uri:
#     mlflow.set_registry_uri(registry_uri)

# try:
#     if experiment_id:
#         mlflow.set_experiment(experiment_id=experiment_id)
#     else:
#         warnings.warn(
#             "No MLflow experiment configured. Set MLFLOW_EXPERIMENT_ID (recommended in Databricks Apps)."
#         )
# except mlflow.exceptions.MlflowException as exc:
#     warnings.warn(
#         f"MLflow experiment setup failed ({exc}). Continuing without forcing experiment selection."
#     )

mlflow.set_tracking_uri("sqlite:///mlflow.db")
try:
    mlflow.set_experiment("Default")
except mlflow.exceptions.MlflowException as exc:
    warnings.warn(f"Failed to set local MLflow experiment to Default: {exc}")
# Enable LangChain tracing so tool calls and LLM responses are captured in MLflow.
mlflow.langchain.autolog()


