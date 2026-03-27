"""
Med-Atlas-AI Agent Setup
=========================
Deploys the UC function and verifies the agent is ready.

Steps (all run automatically):
  1. Load environment from .env
  2. Check all required Python dependencies
  3. Check all required files are present
  4. Execute setup_uc_function.sql on Databricks
     (CREATE OR REPLACE FUNCTION — safe to run repeatedly; overwrites)

Run:
  uv run python ai_agent/setup.py

Prerequisites:
  - Databricks CLI authenticated: `databricks auth login`
  - .env file present with correct HOST, TOKEN, CATALOG, SCHEMA
  - Tables exist: med_atlas_ai.default.{facility_records, facility_facts, regional_insights}
  - Genie space created: Healthcare Facilities Insights
  - Vector Search index created
  - SQL warehouse running (needed for statement execution)
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

AGENT_DIR = Path(__file__).parent

print("Med-Atlas-AI Agent Setup")
print("=" * 60)

# Step 1: Load .env
load_dotenv(AGENT_DIR.parent / ".env")

required = [
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "CATALOG",
    "SCHEMA",
    "LLM_ENDPOINT",
    "GENIE_SPACE_ID",
]
missing = [k for k in required if not os.environ.get(k)]
if missing:
    print(f"ERROR: Missing env vars: {missing}")
    sys.exit(1)
print(f"  .env loaded")
print(f"  host    = {os.environ['DATABRICKS_HOST']}")
print(f"  catalog = {os.environ['CATALOG']}")
print(f"  schema  = {os.environ['SCHEMA']}")

# Step 2: Check Python dependencies
for pkg, imp in [
    ("langgraph", "langgraph"),
    ("databricks-langchain", "databricks_langchain"),
    ("langchain-core", "langchain_core"),
    ("mlflow", "mlflow"),
    ("databricks-sdk", "databricks.sdk"),
]:
    try:
        __import__(imp)
        print(f"  {pkg:<25} OK")
    except ImportError as exc:
        print(f"  {pkg:<25} MISSING — {exc}")
        sys.exit(1)

# Step 3: Check files
for f in ["agent.py", "test_agent.py", "deploy_agent.py", "setup_uc_function.sql"]:
    path = AGENT_DIR / f
    if not path.exists():
        print(f"  ERROR: Missing file: {path}")
        sys.exit(1)
    print(f"  {f:<30} OK")
print(f"  All {len([f])} files present")

# Step 4: Deploy UC function
print()
print("Step 4: Deploying UC function on Databricks...")
print("-" * 60)

# Import run_sql lazily to avoid masking ImportError above
sys.path.insert(0, str(AGENT_DIR))
from run_sql import execute_file

sql_file = AGENT_DIR / "setup_uc_function.sql"
print(f"Executing: {sql_file}")
errors = execute_file(sql_file)

if errors:
    print(f"\n{len(errors)} statement(s) had errors:")
    for e in errors:
        print(f"  - {e['statement']}: {e['error']}")
    sys.exit(1)
else:
    print("\n" + "=" * 60)
    print("UC function deployed successfully.")
    print("The function med_atlas_ai.default.analyze_medical_query")
    print("is now available and will be overwritten on subsequent runs.")
    print("=" * 60)
