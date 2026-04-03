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

This pure-SQL Unity Catalog function (`med_atlas_ai.default.analyze_medical_query`) handles complex anomaly detection and gap analysis. It contains 6 distinct logic branches:

| Branch | Check | Mechanism |
|---|---|---|
| **1. Unmet Needs (Regional Gaps)** | Service Availability | Uses `ARRAY_EXCEPT` to find globally known medical specialties that are definitively missing from a specific region. Leaves free-text procedures/equipment to the LLM for semantic evaluation. |
| **2. Duplicate Facilities** | Data Integrity | Identifies facilities with exact name matches clustered in the same region. |
| **3. Anomaly Flagging** | Statistical Outliers | Applies 3-sigma (standard deviation) checks on capacity and doctor counts to flag extreme medical claims mathematically. |
| **4. Feature Mismatch** | Plausibility | Calculates the ratio of claimed procedures to available equipment. Flagged for LLM review to diagnose overclaiming (e.g., claiming 15 procedures with 0 tracked tools). |
| **5. NGO Overlap** | Effort Duplication | Identifies clusters of 2+ NGOs with the exact same mission/affiliation operating in the exact same city. |
| **6. Problem Classification** | Systemic Failure | Filters for facilities missing entire core pillars (Specialties, Procedures, Equipment) and asks the LLM to diagnose `equipment_gap`, `service_gap`, etc. |

### 📍 Geospatial Payload Enrichment
All relevant SQL branches (Anomaly Flagging, Feature Mismatch, Problem Classification) explicitly return `facility_id`, `latitude`, and `longitude` in their JSON output. This allows the frontend map to immediately pan to and highlight any facility the LLM mentions in its analysis.

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

*Note: All geospatial branches return `facility_id`, `latitude`, and `longitude` natively, enabling immediate map rendering.*

---

---

## 🌐 4. Modular API Architecture (`ai_agent/api/`)

The server has been refactored into a scalable, modular structure to support both the LLM Agent and the Frontend Map UI.

### Directory Structure
- `api/main.py`: Entry point that assembles FastAPI routers and middleware.
- `api/routes/`:
    - `agent.py`: LLM orchestration endpoints (`/invoke`, `/tools`).
    - `map.py`: Backend logic for the Map UI (`/map/search`, `/map/metadata`).
- `api/schemas/`: Pydantic models for request/response validation.
- `api/services/`:
    - `databricks_sql.py`: Lightweight wrapper using the **Databricks Statement Execution API** for high-performance REST queries.

---

## 🗺️ 5. Map UI Backend API (`/map/`)

These endpoints power the interactive map interface for Ghana's healthcare infrastructure.

### `GET /map/metadata`
- **Purpose**: Populates frontend filters (dropdowns, multi-selects).
- **Dynamic Content**: Fetches all unique `state` (regions), `city` labels, and `specialties` directly from the database.
- **Static Content**: Returns standardized lists for `facility_type`, `operator_type`, `organization_type`, and `affiliation_types`.

### `POST /map/search`
- **Purpose**: Returns facility markers and summary cards based on user filters.
- **Payload Example**:
  ```json
  {
      "region": "Greater Accra Region",
      "city": "Accra",
      "specialties": ["Cardiology", "Dentistry"],
      "facility_type": "hospital",
      "operator_type": "public",
      "affiliation_types": ["government"],
      "bbox": [5.5, -0.3, 5.7, -0.1]
  }
  ```
- **Features**:
    - **Viewport Bounding Box Filtering**: If a `bbox` (`[min_lat, min_lon, max_lat, max_lon]`) is provided, the API uses SQL `BETWEEN` operators to rapidly filter facilities strictly to the user's current map camera view.
    - **Advanced Array Filtering**: Uses `ARRAYS_OVERLAP` in SQL to efficiently filter multi-value fields like specialties.
    - **Count**: Returns a `count` field for the "Results Found" UI counter.

### `GET /map/facility/{facility_id}`
- **Purpose**: Fetches the complete medical profile for a single facility when clicked on the map.

---

## 🧪 6. LLM Agent Endpoints

* **`POST /invoke`**: Primary endpoint for conversational AI interaction.
* **`GET /health`**: Returns system status and tool availability.
* **`GET /tools`**: Returns the JSON schema for all agentic tools.

---

## 🗺️🤖 7. Map-LLM Integration (Two-Way Sync)

The architecture is uniquely designed to support **Two-Way Synchronization** between the Map UI and the Conversational Agent:

1. **Map drives the LLM**: Operations performed on the map (like moving the bounding box or applying filters) can be injected into the LLM context.
2. **LLM drives the Map (Citation Sync)**: 
   - When the agent uses `medical_agent_tool` or `geospatial_query_tool`, the SQL engine returns `facility_id`, `latitude`, and `longitude`.
   - The parsers in `agent.py` capture these exact coordinates and embed them into the structured `citations` array.
   - When the Frontend receives the `/invoke` streaming response, it parses these citations and automatically plots, pans to, or pulses the pins for any facility the LLM decided to talk about in its response.

---

## 💻 8. Local Development & Deployment

### Implicit Authentication
The API utilizes the **Databricks Default Credentials** chain. By calling `load_dotenv()`, the system automatically picks up `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from your `.env` file without requiring manual wiring in the code.

### Running the Local Server
```bash
# Start the server (aliased via ai_agent.server for backwards compatibility)
uv run uvicorn ai_agent.server:app --reload --port 8000
```

### Applying SQL Updates
Sync your analytic functions to Unity Catalog:
```bash
uv run python ai_agent/run_sql.py ai_agent/setup_uc_function.sql
uv run python ai_agent/run_sql.py ai_agent/setup_geospatial.sql
```

### Deployment
Deploy as a Databricks App or Model Serving endpoint:
```bash
uv run python ai_agent/deploy_agent.py
```
*(Check `databricks.yml` for environment-specific configurations.)*
