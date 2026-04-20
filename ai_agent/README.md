# Med-Atlas-AI Agent Pipeline Documentation

Welcome to the Med-Atlas-AI Agent documentation. This directory (`ai_agent`) contains the core orchestration, routing, and analytic logic that powers the Med-Atlas-AI healthcare assistant.

Our architecture is built on a **Hybrid Reasoning Engine** philosophy: **"SQL for strict math, LLM for medical reasoning."** Instead of asking the LLM to count, average, or calculate standard deviations (which it does poorly), we offload intensive quantitative operations to Unity Catalog SQL functions. The LLM is then used exclusively to interpret the resulting data using medical domain knowledge.

---

## 1. Agent Architecture & Query Routing

Queries are intercepted by our LangChain agent (`agent.py`) and routed dynamically based on natural language intent:

1. **Quantitative / Ad-hoc Queries (`IS_QUANTITATIVE`)**:
   - Example: "How many hospitals are in Accra?"
   - Router: **Databricks Genie (`genie_chat_tool`)**
   - Logic: Translates natural language to SQL on the fly to count/aggregate clean schema data.
2. **Structural / Analytic Queries (`IS_ANALYTIC`)**:
   - Example: "Which facilities have suspicious overclaims for surgery?"
   - Router: **Medical Agent Engine (`medical_agent_tool`)** or **Geospatial Engine**
   - Logic: Executes highly optimized, pre-computed pure-SQL branches for anomaly detection and geospatial clustering within Unity Catalog.
3. **Semantic / Knowledge Queries (`IS_SEMANTIC`)**:
   - Example: "Which facilities provide cardiac surgery?"
   - Router: **Vector Search (`vector_search_tool`)**
   - Logic: Performs semantic similarity search over pre-generated facility facts stored in the `facility_facts` table.
4. **Geospatial Queries (`IS_GEOSPATIAL`)**:
   - Example: "Find clinics within 30 km of Kumasi."
   - Router: **Geospatial Engine (`geospatial_query_tool`)**
   - Logic: Executes ST_DistanceSpheroid calculations on the WGS84 spheroid via a Unity Catalog SQL function.
5. **Out-of-Scope**:
   - Logic: Blocked natively via system prompt refusal.

---

## 2. The Medical Agent Engine (`setup_uc_function.sql`)

This pure-SQL Unity Catalog function (`med_atlas_ai.default.analyze_medical_query`) handles complex anomaly detection and gap analysis. It contains **5 distinct logic branches**. All branches operate exclusively on `facility_records` — no joins against the high-volume `facility_facts` table. Branch dispatch is driven by `RLIKE` keyword matching on the `query` string passed from the agent.

| Branch                             | Check                | Trigger Keywords                                                                                                                                     | Mechanism                                                                                                                                                                                                                                                                                             |
| ---------------------------------- | -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Unmet Needs (Regional Gaps)** | Service Availability | `unmet`, `gap`, `need`, `service gap`                                                                                                                | Uses `ARRAY_EXCEPT` to find globally known medical specialties definitively missing from a specific region. Returns `specialties_missing` as a pre-computed SQL list — these are confirmed gaps. Free-text procedures and equipment are returned as-is for LLM medical reasoning.                     |
| **2. Anomaly Flagging**            | Statistical Outliers | `outlier`, `anomal`, `flag`, `unusual`, `inconsisten`, `signal`                                                                                      | Applies 3-sigma checks on capacity and doctor counts against a global baseline. Returns `plain_reason` fields describing each outlier in natural language. Caps at 100 results ordered by deviation magnitude.                                                                                        |
| **3. Deep Validation**             | Consistency          | `deep valid`, `validate`, `consistency`, `verify claim`, `mismatch`, `feature mismatch`, `procedure count`, `equipment count`, `infrastr`, `capable` | Region-scoped. SQL exports full facility profiles (specialties, procedures, equipment as comma-separated strings) with a `completeness` tag. Requires `region`, `facility_id`, or `facility_name`. Python agent batches **20 facilities at a time** through the LLM for medical consistency analysis. |

### Geospatial Payload Enrichment

All SQL branches (Anomaly Flagging, Problem Classification, Deep Validation) explicitly return `facility_id`, `latitude`, and `longitude` in their JSON output. This allows the frontend map to immediately pan to and highlight any facility the LLM mentions in its analysis.

### The Missing Data Philosophy (NULL vs Zero)

A major design pillar of the Medical Agent Engine is explicitly preventing the LLM from hallucinating anomalies due to scraped data gaps.

- The SQL differentiates between **`missing_data`** (the field is `NULL` in the database — data was never collected) and **`true_zero`** (the field exists but has no entries — confirmed absence of capability).
- Branches compute and return a **`data_coverage_summary`** payload (e.g., "We only have equipment data for 18% of facilities"). The system prompt mandates that the LLM state these caveats honestly to the user before listing any findings.

---

## 3. The Geospatial Engine (`setup_geospatial.sql`)

Handles distance calculations and spatial clustering using `ST_DistanceSpheroid` (WGS84 spheroid) for geodesic accuracy. The Python agent dynamically geocodes any reference location name (e.g., "Accra") via the LocationIQ API before invoking the UC function.

> **Why ST_DistanceSpheroid over Haversine?**
> Standard formulas like Haversine calculate distance across a perfect sphere, which introduces an error rate of up to 0.5% (distorting distances by several kilometers over long routes). By utilizing Databricks's native `ST_DistanceSpheroid` traversing the WGS84 Reference Spheroid model, the engine accounts for the Earth's equatorial bulge to provide **sub-meter accuracy**, making it the gold standard for straight-line geospatial mapping.

It includes 3 analysis branches:

**Nearby Analysis (`nearby`)**:

- Takes a reference lat/lon and `radius_km`.
- Returns all facilities within the radius sorted by ascending distance, with the exact `distance_km` per facility.
- Supports an optional `condition` keyword which is matched against both `facility_facts.fact_text` (full-text) and `facility_records.specialties`/`procedures` arrays.

---

---

## 4. Modular API Architecture (`ai_agent/api/`)

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

## 5. Map UI Backend API (`/map/`)

These endpoints power the interactive map interface for Ghana's healthcare infrastructure.

### `GET /map/metadata`

- **Purpose**: Populates frontend filters (dropdowns, multi-selects).
- **Source**: All data is loaded entirely from a static `location.json` file co-located with the route. No SQL warehouse query is executed for this endpoint — the response is instantaneous.
- **Content**: Returns `regions` (list), `cities_by_region` (map), `specialties` (list), `facility_types`, `operator_types`, `organization_types`, and `affiliation_types`.

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

### `GET /map/facility/{identifier}`

- **Purpose**: Fetches the complete medical profile for a single facility by ID or by name.
- **Lookup Logic**: The `identifier` path parameter is matched against both `facility_id` (exact match) and `facility_name` (case-insensitive, with automatic whitespace normalization — multiple spaces are collapsed to a single space before comparison).
- **Example URLs**:
  - `GET /map/facility/fac-123-abc`
  - `GET /map/facility/Korle-Bu%20Teaching%20Hospital`

---

## 6. LLM Agent Endpoints

- **`POST /invoke`**: Primary endpoint for conversational AI interaction.
- **`GET /health`**: Returns system status and tool availability.
- **`GET /tools`**: Returns the JSON schema for all agentic tools.

---

## 7. Map-LLM Integration (Two-Way Sync)

The architecture is uniquely designed to support **Two-Way Synchronization** between the Map UI and the Conversational Agent:

1. **Map drives the LLM**: Operations performed on the map (like moving the bounding box or applying filters) can be injected into the LLM context.
2. **LLM drives the Map (Citation Sync)**:
   - When the agent uses `medical_agent_tool` or `geospatial_query_tool`, the SQL engine returns `facility_id`, `latitude`, and `longitude`.
   - The parsers in `agent.py` capture these exact coordinates and embed them into the structured `citations` array.
   - When the Frontend receives the `/invoke` streaming response, it parses these citations and automatically plots, pans to, or pulses the pins for any facility the LLM decided to talk about in its response.

---

## 8. Local Development & Deployment

### Implicit Authentication

The API utilizes the **Databricks Default Credentials** chain. By calling `load_dotenv()`, the system automatically picks up `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from your `.env` file without requiring manual wiring in the code.

### Running the Local Server

```bash
# Start the server (aliased via ai_agent.server for backwards compatibility)
uv run uvicorn ai_agent.server:app --reload --port 8000
```
