# Med-Atlas-AI Agent Pipeline Documentation

Welcome to the Med-Atlas-AI Agent documentation. This directory (`ai_agent`) contains the core orchestration, routing, and analytic logic that powers the Med-Atlas-AI healthcare assistant.

Our architecture is built on a **Hybrid Reasoning Engine** philosophy: **"SQL for strict math, LLM for medical reasoning."** Instead of asking the LLM to count, average, or calculate standard deviations (which it does poorly), we offload intensive quantitative operations to Unity Catalog SQL functions. The LLM is then used exclusively to interpret the resulting data using medical domain knowledge.

---

## 🧠 1. Agent Architecture & Query Routing

Queries are intercepted by our LangChain agent (`agent.py`) and routed dynamically based on natural language intent:

1. **Quantitative / Ad-hoc Queries (`IS_QUANTITATIVE`)**:
   - *Example:* "How many hospitals are in Accra?"
   - *Router:* **Databricks Genie (`genie_chat_tool`)**
   - *Logic:* Translates natural language to SQL on the fly to count/aggregate clean schema data.
   
2. **Structural / Analytic Queries (`IS_ANALYTIC`)**:
   - *Example:* "Which facilities have suspicious overclaims for surgery?"
   - *Router:* **Medical Agent Engine (`medical_agent_tool`)** or **Geospatial Engine**
   - *Logic:* Executes highly optimized, pre-computed pure-SQL branches for anomaly detection and geospatial clustering within Unity Catalog.

3. **Semantic / Knowledge Queries (`IS_SEMANTIC`)**:
   - *Example:* "What are the standard guidelines for setting up a rural clinic?"
   - *Router:* **Vector Search (`document_retrieval_tool`)**
   - *Logic:* Performs RAG against standard healthcare documentation.

4. **Out-of-Scope (`IS_OUT_OF_SCOPE`)**:
   - *Logic:* Blocked natively via system prompt refusal.

---

## 🔬 2. The Medical Agent Engine (`setup_uc_function.sql`)

This pure-SQL Unity Catalog function (`med_atlas_ai.default.analyze_medical_query`) handles complex anomaly detection and gap analysis. It contains 8 distinct logic branches:

| Branch | Check | Mechanism |
|---|---|---|
| **1. Reliability Scoring** | Data Quality | Starts at 100 points, issues deductions across 7 dimensions (Fact density, beds, doctors, capabilities). Generates user-friendly deduction logs. |
| **2. Unmet Needs (Regional Gaps)** | Service Availability | Uses `ARRAY_EXCEPT` to find globally known medical specialties that are definitively missing from a specific region. Leaves free-text procedures/equipment to the LLM for semantic evaluation. |
| **3. Duplicate Facilities** | Data Integrity | Identifies facilities with exact name matches clustered in the same region. |
| **4. Anomaly Flagging** | Statistical Outliers | Applies 3-sigma (standard deviation) checks on capacity and doctor counts to flag extreme medical claims mathematically. |
| **5. Feature Mismatch** | Plausibility | Calculates the ratio of claimed procedures to available equipment. Flagged for LLM review to diagnose overclaiming (e.g., claiming 15 procedures with 0 tracked tools). |
| **6. NGO Overlap** | Effort Duplication | Identifies clusters of 2+ NGOs with the exact same mission/affiliation operating in the exact same city. |
| **7. Problem Classification** | Systemic Failure | Filters for facilities missing entire core pillars (Specialties, Procedures, Equipment) and asks the LLM to diagnose `equipment_gap`, `service_gap`, etc. |
| **8. Data Staleness** | Age verification | Classifies the `updated_at` timestamp as current, moderate, aging, or stale (1yr+). |

### ⚠️ The Missing Data Philosophy (NULL vs Zero)
A major design pillar of the Medical Agent Engine is explicitly preventing the LLM from hallucinating anomalies due to scraped data gaps.
- The SQL differentiates between **`missing_data`** (the tool failed to scrape the equipment field, it is `NULL`) and **`true_zero`** (the tool scraped it and found nothing, or the user input 0).
- Branches compute and pass a **`data_coverage_summary`** payload (e.g., *"We only have equipment data for 18% of facilities"*). The `SYSTEM_PROMPT` mandates that the LLM state these caveats honestly to the end-user rather than treating missing database fields as medical malpractice.

---

## 🌍 3. The Geospatial Engine (`setup_geospatial.sql`)

Handles distance calculations and spatial clustering using `ST_DistanceSpheroid` (WGS84 spheroid) for sub-meter accuracy over standard Haversine calculations. It includes 3 branches:

1. **Nearby Analysis (`nearby`)**:
   - Takes a reference lat/lon and `radius_km`.
   - Returns all facilities within the radius, natively calculating the exact distance.
2. **Cold Spot Analysis (`cold_spot`)**:
   - Groups all facilities by geographic region (State/Country).
   - Identifies regions that contain **zero** matching facilities for a requested medical condition.
3. **Urban/Rural Gap Analysis (`urban_rural`)**:
   - Takes a dynamic array of "Urban Hubs".
   - Calculates the distance of every medical facility to its absolutely nearest provided hub, aiding in resource accessibility research.

---

## 🌐 4. FastAPI Server Endpoints (`server.py`)

We wrap the Med-Atlas-AI LangChain agent in a FastAPI application to serve it externally while preserving MLflow evaluation traces.

* **`GET /health`**
  - **Purpose:** Standard health check.
  - **Returns:** API status, Agent name, LLM Endpoint target, and a list of registered tools.

* **`GET /tools`**
  - **Purpose:** Discovery endpoint.
  - **Returns:** A JSON schema dump of every tool available to the agent (name, description, required parameters).

* **`POST /invoke`**
  - **Purpose:** Primary execution endpoint (replacing CLI `AGENT.predict`). 
  - **Payload:** Accepts an array of conversation `messages` and an optional `user_id` for conversational memory tracking.
  - **Returns:** A parsed response object separating `function_call` (tool usage intent), `function_call_output` (raw SQL JSON data), and `message` (final LLM synthesized markdown answer).

---

## 💻 5. Local Development & Deployment

### Running the Local Server
```bash
# Start the FastAPI uvicorn server with auto-reload disabled (or adjust reload=True)
uv run python -m ai_agent.server
```

### Applying SQL Updates to Databricks Unity Catalog
If you modify the logic inside `setup_uc_function.sql` or `setup_geospatial.sql`, you must sync the function to Databricks:
```bash
uv run python ai_agent/run_sql.py ai_agent/setup_uc_function.sql
uv run python ai_agent/run_sql.py ai_agent/setup_geospatial.sql
```

### Deploying the Agent
The agent is bundled and deployed via Databricks Model Serving. To deploy/update:
```bash
uv run python ai_agent/deploy_agent.py
```
*(Ensure your `databricks.yml` and `deploy-agent.yml` configurations map to the correct endpoint variables.)*
