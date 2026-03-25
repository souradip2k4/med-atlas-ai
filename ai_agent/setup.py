"""
Med-Atlas-AI Agent Setup
========================
Run this once to create the UC function and verify the agent is ready.

Steps:
  1. Create the medical reasoning UC function on Databricks
     (run the SQL via a Databricks notebook or `databricks sql -f`)

  2. Test agent locally
     (optional, requires Databricks Connect auth)
     uv run python agent/test_agent.py

  3. Deploy to Databricks Model Serving
     uv run python agent/deploy_agent.py

Prerequisites:
  - Databricks CLI authenticated: `databricks auth login`
  - .env file present with correct HOST, TOKEN, CATALOG, SCHEMA
  - Tables exist: med_atlas_ai.default.{facility_records, facility_facts, regional_insights}
  - Genie space created: Healthcare Facilities Insights (01f127545dfc197098d3b09ba7042f69)
  - Vector Search index created: med_atlas_ai.default.facility_facts_index
  - SQL warehouse running (needed for system.ai.python_exec)

Note: The custom UC function (setup_uc_function.sql) is no longer required —
  medical_agent_tool now uses the built-in system.ai.python_exec UC function.
"""

from pathlib import Path

AGENT_DIR = Path(__file__).parent

print("Med-Atlas-AI Agent Setup")
print("=" * 60)

# Step 1: Verify .env
from dotenv import load_dotenv
load_dotenv(AGENT_DIR.parent / ".env")

required = ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "CATALOG", "SCHEMA",
            "LLM_ENDPOINT", "GENIE_SPACE_ID"]
missing = [k for k in required if not __import__("os").environ.get(k)]
if missing:
    print(f"ERROR: Missing env vars: {missing}")
    exit(1)
print(f"  ✓ .env loaded (host={__import__('os').environ['DATABRICKS_HOST']})")

# Step 2: Check Python deps
try:
    import langgraph
    import databricks_langchain
    import langchain_core
    import mlflow
    lg_version = getattr(langgraph, "__version__", "installed")
    print(f"  ✓ Dependencies installed (langgraph={lg_version})")
except ImportError as e:
    print(f"  ERROR: Missing dependency — {e}")
    print(f"  Run: uv add langgraph databricks-langchain langchain-core mlflow-skinny")
    exit(1)

# Step 3: Check files
files = ["agent.py", "test_agent.py", "deploy_agent.py", "setup_uc_function.sql"]
for f in files:
    path = AGENT_DIR / f
    if not path.exists():
        print(f"  ERROR: Missing file: {path}")
        exit(1)
print(f"  ✓ All {len(files)} agent files present")

print()
print("Setup checklist:")
print("  [ ] Verify SQL warehouse is running:")
print("       uv run python -c \"from databricks.sdk import WorkspaceClient; ...")
print("       (Warehouse 'Serverless Starter Warehouse' should be RUNNING)")
print()
print("  [ ] Test locally:")
print(f"       uv run python {AGENT_DIR / 'test_agent.py'}")
print()
print("  [ ] Deploy to Databricks:")
print(f"       uv run python {AGENT_DIR / 'deploy_agent.py'}")
print()
print("Done.")
