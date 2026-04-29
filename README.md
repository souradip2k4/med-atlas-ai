# Med-Atlas-AI

An end-to-end healthcare infrastructure intelligence platform for Ghana. The system comprises two tightly integrated subsystems: an **IDP Pipeline** that transforms raw facility data into structured Delta tables with vector-searchable facts, and an **AI Agent** that answers complex medical, statistical, and geospatial queries using a hybrid SQL + LLM reasoning engine.

> **Navigate:** [Part I — IDP Pipeline](#part-i--intelligent-document-processing-idp-pipeline) · [Part II — AI Agent Pipeline](#part-ii--ai-agent-pipeline) · [Part III — Frontend](#part-iii--frontend)

---

## Quick Start

### Environment Variables

Create a `.env` file in the `IDP/` directory:

```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
DATABRICKS_SERVERLESS=true
CATALOG=med_atlas_ai_v2
SCHEMA=default
CSV_PATH=Virtue Foundation Ghana v0.3 - Sheet1.csv
LLM_ENDPOINT=databricks-gpt-oss-120b
MAX_WORKERS=6                               # Parallel extraction threads
MAX_PROCESS_ROWS=797                        # Max unique rows per run (max = 797)
GEMINI_API_KEY=AIzaXXXXXXXXXXXXXXXXXXXXX   # Gemini location inference
LOCATION_IQ_ACCESS_TOKEN=pk.XXXXXXXX       # LocationIQ geocoding
```

---

### 1. Run the IDP Pipeline (Stages 1–6)

```bash
cd IDP

# 1. Create virtual environment
uv venv
source .venv/bin/activate

# 2. Install dependencies
uv pip install -r requirements.txt

# 3. Run Stage 1–4: Extract & persist facility_records
uv run facility_record_generator.py

# 4. Run Stage 5: Generate facility_facts for Vector Search
uv run populate_facts.py

# 5. Run Stage 6: Compute regional_insights for Text-to-SQL
uv run compute_regional_insights.py
```

Each script is **idempotent via full overwrite** — re-running regenerates its target Delta table from scratch. There is no checkpointing. To process more rows, increase `MAX_PROCESS_ROWS` in `.env` and re-run `facility_record_generator.py`.

---

### 2. Set Up Databricks Infrastructure

#### 2a — Enable Change Data Feed on the facts table

Run the following in a Databricks notebook or SQL editor:

```sql
ALTER TABLE med_atlas_ai_v2.default.facility_facts
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
```

This is required for the Vector Search managed sync index to detect row-level changes.

#### 2b — Create a Vector Search Endpoint

In the Databricks sidebar, go to **Compute → Vector Search → Create Endpoint**. Give it a name (e.g., `vector-search-endpoint`) and confirm. Wait for the endpoint to reach the **Online** state before proceeding.

#### 2c — Create the Vector Search Index

1. Navigate to **Catalog → `med_atlas_ai_v2` → `default` → `facility_facts`**
2. Click **Create → Vector Search Index** and fill in the form:

| Field                   | Value                                 |
| ----------------------- | ------------------------------------- |
| Index name              | `med_atlas_vs_endpoint_v2`            |
| Primary key             | `fact_id`                             |
| Columns to index        | _(leave blank — indexes all columns)_ |
| Vector Search endpoint  | _(select the endpoint created in 2b)_ |
| Index subtype           | **Hybrid Index**                      |
| Embedding source        | **Compute embeddings**                |
| Embedding source column | `fact_text`                           |
| Embedding model         | `databricks-gte-large-en`             |
| Sync mode               | **Triggered**                         |

3. Click **Create** and wait for the index status to show **Online**.

#### 2d — Create a Genie Space

1. In the Databricks sidebar, navigate to **Genie Spaces** and click **New**
2. In the **Connect your data** dialog, select both `facility_records` and `regional_insights` from `med_atlas_ai_v2 → default`
3. Click **Create**
4. Once created, open the **About** panel of the space and copy the **Space ID** and **Name** — you will need these in the next step

#### 2e — Register the Unity Catalog SQL Functions

Run both SQL files from the Databricks SQL editor or a connected notebook:

- `ai_agent/setup_geospatial.sql` — geospatial radius search function
- `ai_agent/setup_uc_function.sql` — anomaly and gap detection function

---

### 3. Start the AI Agent Server

Create `ai_agent/.env` with the following variables (use the Space ID and warehouse ID from step 2d):

```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
CATALOG=med_atlas_ai_v2
SCHEMA=default
VECTOR_SEARCH_INDEX=med_atlas_ai_v2.default.med_atlas_vs_endpoint_v2  # index created in 2c
ANALYZE_UC_FUNCTION_NAME=med_atlas_ai_v2.default.analyze_medical_query
GEOSPATIAL_UC_FUNCTION_NAME=med_atlas_ai_v2.default.find_facilities_nearby
GENIE_SPACE_ID=<Space ID from step 2d>
GENIE_SPACE_NAME=Healthcare Facilities Insights
LOCATION_IQ_ACCESS_TOKEN=pk.XXXXXXXX
DATABRICKS_WAREHOUSE_ID=<warehouse ID from step 2d>
```

```bash
# Run from the project root
uv run uvicorn ai_agent.server:app --reload --port 8000
```

---

### 4. Start the Frontend

Create `frontend/.env`:

```env
VITE_API_BASE_URL=http://localhost:8000
VITE_MAPBOX_TOKEN=<your-mapbox-public-token>
```

```bash
cd frontend && pnpm install && pnpm dev
# Accessible at http://localhost:5173
```

---

## Part I — Intelligent Document Processing (IDP) Pipeline

[Jump to Part II → AI Agent Pipeline](#part-ii--ai-agent-pipeline)

The IDP (Intelligent Document Processing) Pipeline is the **data backbone** of Med-Atlas-AI. It takes a raw CSV of healthcare facilities, cleans and deduplicates the records, uses an AI model to fill in missing structured details, resolves locations and coordinates, and ultimately stores everything in three organised database tables — ready for the AI Agent to query.

Think of it as the pipeline that turns messy, incomplete raw data into a clean, structured, and searchable knowledge base about Ghana's healthcare infrastructure.

---

## Technology Stack

| Layer                         | Technology                                                                         |
| ----------------------------- | ---------------------------------------------------------------------------------- |
| **LLM — Column Extraction**   | Databricks Model Serving · `databricks-gpt-oss-120b`                               |
| **LLM — Location Inference**  | Google Gemini API · `gemini-3.1-flash-lite-preview`                                |
| **Structured Output Parsing** | **Pydantic v2** — on all LLM responses                                             |
| **Geocoding**                 | **LocationIQ API** via `geopy.geocoders.Nominatim`-compatible client (2 req/s)     |
| **Vector Embeddings**         | **Databricks Mosaic AI Vector Search** · `databricks-gte-large-en` embedding model |
| **Data Storage**              | **Databricks Delta Lake** via Unity Catalog (3 managed Delta tables)               |
| **Distributed Processing**    | **Apache Spark / PySpark** via Databricks Connect (serverless)                     |
| **Schema Definition**         | PySpark `StructType` schemas (`storage/models.py`)                                 |
| **Parallelism**               | Python `ThreadPoolExecutor` (`MAX_WORKERS` configurable)                           |

---

## Architecture Overview

The pipeline processes data through 6 sequential stages. Each CSV row passes through deduplication, LLM extraction, a 5-step deterministic merge (including Gemini-based location inference and LocationIQ geocoding), fact generation, and regional aggregation.

```
CSV File (Ghana healthcare facilities — 987 raw rows)
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 1 — Database Initialisation                                       │
│  DatabricksDatabase connects via Databricks Connect.                     │
│  Registers schemas for: facility_records, facility_facts,                │
│  regional_insights.                                                      │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 2 — CSV Loading (loader.py)                                       │
│  Reads the raw CSV with Pandas into memory as List[Dict].                │
│  Normalises column names, replaces NULL tokens with Python None.         │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 2b — Deduplication (deduplicator.py)                              │
│  Groups rows by pk_unique_id. For each group, merges duplicate rows      │
│  by consolidating non-null values column-by-column.                      │
│  Result: 987 raw rows → 797 unique facility records (190 removed).       │
│  MAX_PROCESS_ROWS caps how many unique rows are processed in one run.    │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 3 — LLM Extraction Chain (extractor.py)                           │
│  Each row is synthesised into a text block then passed to the LLM:       │
│    ① Facility Fact Extraction → procedures, equipment,                   │
│       capabilities, specialties, description, addresses,                 │
│       and contact fields.                                                │
│  Model: databricks-gpt-oss-120b via ChatDatabricks.                      │
│  Output JSON validated through Pydantic FacilityFacts model.             │
│  Capability arrays filtered by _GARBAGE_KEYWORDS before downstream use.  │
│  Runs in parallel via ThreadPoolExecutor (configurable MAX_WORKERS).     │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 4 — Merge, Shape & Persist → facility_records (merger.py)         │
│  Consolidates LLM output + CSV row via a 5-step location cascade:        │
│    1. Direct CSV values (city, state).                                   │
│    2. Deterministic _GHANA_CITY_REGION dictionary (100+ cities).         │
│    3. Gemini Case 1: city known, region missing.                         │
│    4. Gemini Case 2: both city AND region missing.                       │
│    5. LocationIQ API (geopy): resolves lat/lon from full address.        │
│  Output → facility_records Delta table (single overwrite at run end)     │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 5 — Fact Generation → facility_facts (facility_fact_generator.py) │
│  Generates ≤6 semantically rich natural-language rows per facility:      │
│    • 1 "summary"     — identity: name, type, location, affiliation       │
│    • 1 "description" — narrative: description + mission_statement        │
│    • 1 "procedure"   — all procedures comma-joined                       │
│    • 1 "equipment"   — all equipment comma-joined                        │
│    • 1 "capability"  — all capabilities comma-joined                     │
│    • 1 "specialty"   — all specialties comma-joined                      │
│  Missing data → row silently skipped (never "Unknown"/"null").           │
│  Rows are embedded by Databricks Mosaic AI Vector Search using the       │
│  databricks-gte-large-en model and stored in a managed Delta sync index. │
│  Output → facility_facts Delta table (single overwrite at run end)       │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 6 — Regional Insights → regional_insights (PySpark aggregations)  │
│  PySpark groupBy aggregations across 3 dimensions:                       │
│    ① overview  — total facilities, beds, doctors per region/city         │
│    ② operator  — public vs private breakdown with bed/doctor counts      │
│    ③ specialty — facility count per specialty per region/city            │
│  Designed exclusively for Text-to-SQL via Databricks Genie.              │
│  Output → regional_insights Delta table (single overwrite at run end)    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
IDP/
├── config/                                 # LLM prompt definitions & Pydantic output models
│   ├── __init__.py
│   ├── free_form.py                        # FacilityFacts Pydantic model + system prompt
│   └── medical_specialties.py             # MedicalSpecialties model (reference)
│
├── pipeline/                               # Core processing pipeline
│   ├── __init__.py
│   ├── loader.py                           # CSV → List[Dict] via Pandas
│   ├── deduplicator.py                     # pk_unique_id deduplication + row merging
│   ├── preprocessor.py                     # Row → synthesised text block for LLM input
│   ├── extractor.py                        # LLM chain + _GARBAGE_KEYWORDS filter
│   ├── merger.py                           # Merges LLM outputs + CSV into facility_records
│   ├── geocoder.py                         # LocationIQ geocoder via geopy (2 req/s)
│   ├── location_resolver.py               # Gemini city/region inference (14 RPM StrictThrottle)
│   └── facility_fact_generator.py          # Generates fact_text rows for Vector Search
│
├── storage/                                # Database layer
│   ├── __init__.py
│   ├── database.py                         # DatabricksDatabase class (Spark session + Delta I/O)
│   └── models.py                           # PySpark StructType schemas for all Delta tables
│
├── facility_record_generator.py            # Pipeline orchestrator (Stages 1–4)
├── populate_facts.py                       # Standalone — runs Stage 5 on all records
├── compute_regional_insights.py            # Standalone — runs Stage 6
└── .env                                    # Credentials & config (gitignored)
```

---

### Project Structure — `ai_agent/`

```
ai_agent/
├── api/                                    # Modular FastAPI layer
│   ├── main.py                             # Assembles routers and middleware
│   ├── routes/
│   │   ├── agent.py                        # /invoke, /tools, /health endpoints
│   │   ├── map.py                          # /map/search, /map/metadata, /map/facility endpoints
│   │   └── location.json                   # Static metadata (regions, cities, specialties)
│   └── schemas/
│       ├── agent.py                        # Pydantic models for /invoke request/response
│       └── map.py                          # Pydantic models for /map request/response
│
├── agent.py                                # LangGraph StateGraph, all 4 tools, SYSTEM_PROMPT
├── server.py                               # FastAPI app entry point (imports api/main.py)
├── setup_geospatial.sql                    # Unity Catalog SQL function: find_facilities_nearby
├── setup_uc_function.sql                   # Unity Catalog SQL function: analyze_medical_query
└── .env                                    # Credentials & config (gitignored)
```

---

## Detailed Pipeline Stages

### Stage 1 — Database Initialisation

**File:** `facility_record_generator.py`, `storage/database.py`

Creates a `DatabricksDatabase` instance which lazily initialises a Spark session via **Databricks Connect**. The session sets the default catalog and schema from `CATALOG` and `SCHEMA` env vars (e.g., `med_atlas_ai_v2.default`).

Schema registration happens at startup for all three Delta tables, but actual table creation is deferred until the first write.

---

### Stage 2 — CSV Loading

**File:** `pipeline/loader.py`

Reads the raw CSV file using **Pandas** with `dtype=str` to preserve all data as strings. Performs two cleaning steps:

1. **Column name normalisation** — strips whitespace, replaces special characters with underscores, lowercases.
2. **NULL token replacement** — converts `"null"`, `"None"`, `"N/A"`, `""`, and `NaN` values into Python `None`.

Returns a `List[Dict[str, Any]]` where each dict is one raw CSV row.

---

### Stage 2b — Deduplication

**File:** `pipeline/deduplicator.py`

The raw CSV contains **987 rows** representing 797 unique facilities. The extra 190 rows are duplicates — the same facility scraped from multiple web sources, resulting in multiple rows sharing the same `pk_unique_id` but with complementary non-null data.

**How it works:** Rows are grouped by `pk_unique_id`. For each group, a single merged row is produced by taking, for each column, the first non-null value across duplicate rows. This ensures data captured from any source page for the same facility is preserved rather than being discarded.

After deduplication, `MAX_PROCESS_ROWS` (from `.env`) slices the 797 unique records to cap LLM cost per run.

---

### Stage 3 — LLM Extraction Chain

**File:** `pipeline/extractor.py`, `pipeline/preprocessor.py`

#### Step 3a — Text Synthesis (`preprocessor.py`)

Each deduplicated CSV row is converted into a structured text block by `synthesize_row_text()`. It iterates over all columns (skipping identity columns), formats non-null values as `"Field Name: value"` lines, and parses JSON arrays into bulleted lists. This synthesised text becomes the single input to the LLM.

#### Step 3b — LLM Extraction (`extractor.py`)

Uses **`ChatDatabricks`** (LangChain integration) pointed at the configured Databricks Model Serving endpoint.

| Config       | Value                                                                                                                 |
| ------------ | --------------------------------------------------------------------------------------------------------------------- |
| Model        | `databricks-gpt-oss-120b` (via `LLM_ENDPOINT` env var)                                                                |
| Prompt       | `config/free_form.py` system prompt                                                                                   |
| Output Model | **Pydantic** `FacilityFacts` (`model_validate_json`)                                                                  |
| Extracts     | `procedures`, `equipment`, `capabilities`, `specialties`, cleaned `facility_name`, `description`, `mission_statement` |

Responses are stripped of markdown fences and parsed via Pydantic's `model_validate_json`. Failed validation logs a warning and returns `None` for that field — downstream stages handle missing data gracefully.

#### Step 3c — Capability Garbage Filtering

After extraction, all array fields (especially `capabilities`) pass through `_clean_array()` using the shared `_GARBAGE_KEYWORDS` list. This is a **case-insensitive substring filter** that strips strings originating from directory listings, contact info, web scraper artifacts, and registration noise (e.g., strings containing `"telephone"`, `"ghanaYello"`, `"listed in"`, `"registered with"`). If all items in an array are filtered out, the field is stored as `null` — never as an empty list.

Rows are processed in **parallel** via `ThreadPoolExecutor` with configurable `MAX_WORKERS`.

---

### Stage 4 — Merge, Shape & Persist → `facility_records`

**File:** `pipeline/merger.py` | **Written by:** `facility_record_generator.py`

The `merge_extraction_results()` function consolidates the LLM extraction output plus the original CSV row into a single flat dictionary, then the pipeline writes all processed records to the **`facility_records` Delta table** as a single overwrite at run end.

#### Merging Strategy

- **Scalar fields** — `_first_non_null()` returns the first non-`None`, non-empty value from a priority list (LLM output preferred over raw CSV).
- **Array fields** — `_merge_arrays()` concatenates and deduplicates entries (case-insensitive, insertion-order preserved). All arrays additionally pass through `_clean_array()` as a safety-net second pass.

#### 5-Step Location Resolution Cascade

City and region (`state`) are resolved with a strict cost-ascending priority cascade to maximise coverage while minimising API spend:

| Step | Method                                        | Triggers When                            |
| ---- | --------------------------------------------- | ---------------------------------------- |
| 1    | Direct CSV values                             | Always attempted first                   |
| 2    | `_GHANA_CITY_REGION` dictionary (100+ cities) | State missing, city present              |
| 3    | **Gemini API** — Case 1                       | City present but not in dictionary       |
| 4    | **Gemini API** — Case 2                       | Both city AND state missing              |
| 5    | **LocationIQ API** (`geopy`)                  | After city/state resolved — lat/lon only |

#### `facility_records` Delta Table Schema

Each row is one unique healthcare facility. Written as a **full overwrite** at the end of each run.

| Column               | Type               | Description                                                                     |
| -------------------- | ------------------ | ------------------------------------------------------------------------------- |
| `facility_id`        | String (PK)        | Set from CSV `pk_unique_id` — stable across runs                                |
| `facility_name`      | String             | Official name of the facility                                                   |
| `organization_type`  | String             | `"facility"` or `"ngo"`                                                         |
| `specialties`        | Array[String]      | Medical specialties offered                                                     |
| `procedures`         | Array[String]      | Medical procedures performed                                                    |
| `equipment`          | Array[String]      | Medical equipment available                                                     |
| `capabilities`       | Array[String]      | Clinical capabilities (garbage-filtered)                                        |
| `address_line1/2/3`  | String             | Street address components                                                       |
| `city`               | String             | City/town                                                                       |
| `state`              | String             | Ghana region (e.g., `"Greater Accra"`)                                          |
| `country`            | String             | Always `"Ghana"`                                                                |
| `country_code`       | String             | ISO code (e.g., `"GH"`)                                                         |
| `latitude`           | Double             | Geocoded via LocationIQ (null if unresolvable)                                  |
| `longitude`          | Double             | Geocoded via LocationIQ (null if unresolvable)                                  |
| `phone_numbers`      | Array[String]      | Contact numbers                                                                 |
| `email`              | String             | Contact email                                                                   |
| `websites`           | Array[String]      | Associated URLs                                                                 |
| `social_links`       | Map[String,String] | Platform → URL (e.g., `{"facebookLink": "url"}`)                                |
| `officialWebsite`    | String             | Primary homepage                                                                |
| `year_established`   | Integer            | Year founded                                                                    |
| `accepts_volunteers` | Boolean            | Accepts clinical volunteers                                                     |
| `capacity`           | Integer            | Bed capacity                                                                    |
| `no_doctors`         | Integer            | Doctor count                                                                    |
| `description`        | String             | Narrative description of the facility                                           |
| `mission_statement`  | String             | Official mission statement                                                      |
| `affiliation_types`  | Array[String]      | `philanthropy-legacy`, `academic`, `faith-tradition`, `government`, `community` |
| `operator_type`      | String             | `"public"` or `"private"`                                                       |
| `facility_type`      | String             | `"clinic"`, `"hospital"`, `"farmacy"`, `"doctor"`, `"dentist"`                  |
| `created_at`         | Timestamp          | Record creation time (UTC)                                                      |
| `updated_at`         | Timestamp          | Record last update time (UTC)                                                   |

---

### Stage 5 — Fact Generation → `facility_facts` (Vector Search)

**File:** `pipeline/facility_fact_generator.py` | **Run via:** `populate_facts.py`

Each facility record from `facility_records` is transformed into up to **6 focused natural-language facts**. This bounded row count prevents "vector crowding" — where top-k retrieval returns 10 rows all about the same hospital.

#### Location Prefix (`loc_str`)

Every `fact_text` is prefixed with a geographic context string built from `city`, `state`, `country` (e.g., `" in Accra, Greater Accra, Ghana"`). This enables geographic similarity matching without requiring structured metadata filters on every query.

#### Fact Types

| `fact_type`   | Source Fields                                                               | Example `fact_text`                                                                                                                        |
| ------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `summary`     | `operator_type`, `facility_type`, `org_type`, `affiliation_types`, location | `"Bromley Park Dental Clinic is a privately operated clinic (facility) in Accra, Greater Accra, Ghana. It is affiliated with: community."` |
| `description` | `description`, `mission_statement`                                          | `"WAAF in Takoradi, Western, Ghana: Committed to maternal health. Its mission is: To serve underserved women."`                            |
| `procedure`   | `procedures` array                                                          | `"1st Foundation Clinic in Accra, … provides the following medical procedures: bloodTransfusion, woundSuturing."`                          |
| `equipment`   | `equipment` array                                                           | `"WAAF in Takoradi, … is equipped with: ultrasoundMachine, xRayMachine."`                                                                  |
| `capability`  | `capabilities` array                                                        | `"3E Medical Center in Accra, … has the following clinical capabilities: 24-hour services, inpatient services."`                           |
| `specialty`   | `specialties` array                                                         | `"Ahmadiyya Hospital in Kumasi, … offers specialty care in: internalMedicine, familyMedicine."`                                            |

> **Why `summary` and `description` are separate:** `summary` is optimised for identity/type/affiliation matching. `description` is optimised for narrative/mission-intent matching. Keeping them separate means the Vector Search index retrieves the right semantic signal per query intent without cross-contamination.

**Missing data:** If a field is `None` or empty, the corresponding row is silently skipped. The words `"Unknown"` and `"null"` are **never** inserted into any `fact_text`.

#### Vector Search Integration

After `populate_facts.py` writes the `facility_facts` Delta table, a **Databricks Mosaic AI Vector Search** managed sync index automatically picks up changes via Change Data Feed (CDF) and generates dense vector embeddings for every `fact_text` using the **`databricks-gte-large-en`** model. The `fact_type` column is stored as metadata, enabling filtered ANN searches at query time (e.g., `"MRI machine" + filter: fact_type = "equipment"`).

#### `facility_facts` Delta Table Schema

Written as a **full overwrite** by `populate_facts.py`. Regenerates the complete table from all current `facility_records` every run.

| Column        | Type        | Description                                                                                  |
| ------------- | ----------- | -------------------------------------------------------------------------------------------- |
| `fact_id`     | String (PK) | UUID for this fact row                                                                       |
| `facility_id` | String (FK) | → `facility_records.facility_id`                                                             |
| `fact_text`   | String      | Natural language sentence — embedded by Vector Search                                        |
| `fact_type`   | String      | `"summary"`, `"description"`, `"procedure"`, `"equipment"`, `"capability"`, or `"specialty"` |

---

### Stage 6 — Regional Insights Aggregation → `regional_insights` (Text-to-SQL)

**File:** `compute_regional_insights.py`

Builds a pre-aggregated OLAP analytics table using **PySpark** `groupBy` aggregations on `facility_records`. Designed **exclusively for Text-to-SQL** via Databricks Genie — it is **not** used for Vector Search.

#### Six Aggregation Dimensions

| #   | `insight_category` | `insight_value`                                                                 | `facility_count` | `total_capacity` | `total_doctors` |
| --- | ------------------ | ------------------------------------------------------------------------------- | ---------------- | ---------------- | --------------- |
| 1   | `overview`         | `all_facilities`                                                                | ✅ countDistinct | ✅ SUM           | ✅ SUM          |
| 2   | `operator`         | `public` / `private`                                                            | ✅ countDistinct | ✅ SUM           | ✅ SUM          |
| 3   | `facility_type`    | `clinic`, `hospital`, `farmacy`, `doctor`, `dentist`                            | ✅ countDistinct | ✅ SUM           | ✅ SUM          |
| 4   | `organization`     | `facility` / `ngo`                                                              | ✅ countDistinct | ✅ SUM           | ✅ SUM          |
| 5   | `specialty`        | e.g. `"cardiology"`                                                             | ✅ countDistinct | `null`\*         | `null`\*        |
| 6   | `affiliation`      | `faith-tradition`, `government`, `community`, `philanthropy-legacy`, `academic` | ✅ countDistinct | `null`\*         | `null`\*        |

> \*`total_capacity` and `total_doctors` are explicitly `NULL` for array-based dimensions (`specialty`, `affiliation`) to prevent statistical overcounting — a hospital's bed count must not be multiplied by its number of specialties or affiliations.

**Geographic dimensions:** Each aggregation row covers both state-level totals (`city IS NULL`) and city-level breakdowns (`city IS NOT NULL`), so Genie can answer both "how many facilities in Greater Accra?" and "how many facilities specifically in Accra city?" from the same table.

#### `regional_insights` Delta Table Schema

| Column             | Type    | Description                                                                                                         |
| ------------------ | ------- | ------------------------------------------------------------------------------------------------------------------- |
| `country`          | String  | Always `"Ghana"`                                                                                                    |
| `state`            | String  | Ghana region (grouping dimension)                                                                                   |
| `city`             | String  | City (null = state-level aggregate)                                                                                 |
| `insight_category` | String  | `"overview"`, `"operator"`, `"facility_type"`, `"organization"`, `"specialty"`, or `"affiliation"`                  |
| `insight_value`    | String  | Depends on category — e.g. `"all_facilities"`, `"public"`, `"clinic"`, `"ngo"`, specialty name, or affiliation name |
| `facility_count`   | Integer | Distinct facilities in this slice                                                                                   |
| `total_capacity`   | Integer | SUM of bed capacity (null for array-based dimensions: specialty, affiliation)                                       |
| `total_doctors`    | Integer | SUM of doctor counts (null for array-based dimensions: specialty, affiliation)                                      |

#### Example Genie Text-to-SQL Queries

```sql
-- "How many facilities are in Accra?"
SELECT facility_count FROM regional_insights
WHERE city = 'Accra' AND insight_category = 'overview';

-- "Compare public vs private hospital capacity in Ashanti"
SELECT insight_value, facility_count, total_capacity, total_doctors
FROM regional_insights
WHERE state = 'Ashanti' AND insight_category = 'operator';

-- "Which regions have the most ophthalmology centres?"
SELECT state, facility_count FROM regional_insights
WHERE insight_category = 'specialty' AND insight_value = 'ophthalmology'
ORDER BY facility_count DESC;

-- "How many government-affiliated clinics are there in Greater Accra?"
SELECT f.facility_count as total_clinics, a.facility_count as gov_affiliated
FROM regional_insights f
JOIN regional_insights a ON f.state = a.state AND f.city = a.city
WHERE f.state = 'Greater Accra'
  AND f.insight_category = 'facility_type' AND f.insight_value = 'clinic'
  AND a.insight_category = 'affiliation' AND a.insight_value = 'government';
```

---

## Dual Retrieval Architecture

| Query Type                     | Retrieval Method     | Table Used                                 | Example Query                                               |
| ------------------------------ | -------------------- | ------------------------------------------ | ----------------------------------------------------------- |
| **Semantic / Qualitative**     | Vector Search (RAG)  | `facility_facts`                           | _"Find clinics near Accra that do cardiac surgery"_         |
| **Narrative / Mission**        | Vector Search (RAG)  | `facility_facts` (`fact_type=description`) | _"Find facilities focused on maternal and child health"_    |
| **Quantitative / Statistical** | Text-to-SQL (Genie)  | `regional_insights`                        | _"How many public hospitals are in Ashanti?"_               |
| **Row-level Lookup**           | Direct SQL (Genie)   | `facility_records`                         | _"List names and phone numbers of public clinics in Accra"_ |
| **Anomaly Detection**          | Agent-side LLM + SQL | Both tables                                | _"Which clinics claim surgeries but have no beds?"_         |

---

## Key Design Decisions

| Decision                                                 | Rationale                                                                                                                                                                                                              |
| -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Pydantic for all LLM outputs**                         | Guarantees schema-safe extraction — invalid JSON or missing fields trigger a logged warning and a graceful `None`, preventing downstream crashes.                                                                      |
| **`summary` and `description` as separate `fact_type`s** | `summary` is tuned for identity/type/location matching; `description` for narrative/mission intent. Keeping them separate eliminates cross-contamination in top-k retrieval.                                           |
| **No `"Unknown"` or `"null"` in `fact_text`**            | If data is missing, the row is silently skipped. This prevents the Vector DB from returning false-positive matches on the literal word "unknown".                                                                      |
| **Geographic context in every `fact_text`**              | City, state, and country are embedded directly into every sentence, enabling geographic similarity matching without requiring structured metadata filters on every query.                                              |
| **5-step location cascade (dict → Gemini → LocationIQ)** | Deterministic lookup is free and instant; Gemini only fires for edge cases; LocationIQ is called last (lat/lon only) to protect its quota. Maximum coverage, minimum cost.                                             |
| **`StrictThrottle` over Token Bucket for Gemini**        | A Token Bucket allows burst-firing (all workers immediately consume tokens). `StrictThrottle` enforces one call per 4.28 s across all workers—mathematically guaranteed to stay under the 15 RPM rolling-window quota. |
| **`_GARBAGE_KEYWORDS` as a shared constant**             | Both `extractor.py` and `merger.py` import the same list, guaranteeing consistent filtering at extraction (first pass) and merge (safety-net pass).                                                                    |
| **Deduplication preserves complementary data**           | Different scrape sources for the same facility often have complementary non-null fields. Column-wise merge (first non-null wins) maximises information density per facility.                                           |
| **Single final overwrite per script**                    | Mid-run batched overwrites silently destroy earlier batches. Writing once at the very end guarantees the Delta table always reflects the full processed output.                                                        |
| **`regional_insights` uses Text-to-SQL, not RAG**        | Vector Search is mathematically unreliable for counting and aggregation. Text-to-SQL on a pre-aggregated table gives provably correct quantitative answers.                                                            |
| **Overcounting prevention in `regional_insights`**       | `total_capacity` and `total_doctors` are `NULL` for the `specialty` dimension — a hospital's bed count must not be multiplied by its number of specialties.                                                            |
| **`facility_id = CSV pk_unique_id`**                     | Eliminates a separate `source_row_id` column. The deduplicator guarantees exactly one merged row per `pk_unique_id`, making this ID stable and collision-free across all tables.                                       |
| **Three independent runner scripts**                     | `facility_record_generator.py`, `populate_facts.py`, and `compute_regional_insights.py` are fully decoupled. Any single stage can be re-run in isolation without re-triggering expensive LLM extraction.               |

## Part II — AI Agent Pipeline

[← Back to Part I — IDP Pipeline](#part-i--intelligent-document-processing-idp-pipeline)

The AI Agent is a conversational intelligence layer built on top of the three Delta tables populated by the IDP Pipeline. It accepts natural language questions about Ghana's healthcare infrastructure — ranging from "Which clinics in Kumasi perform cardiac surgery?" to "Where are the largest geographic cold spots for emergency care within 50 km?" — and returns medically grounded, evidence-backed answers. Rather than sending every question blindly to an LLM, the agent first **classifies the intent** of each query and then **routes it** to the most appropriate computational tool: a geospatial SQL engine, a semantic vector search index, a pure-SQL anomaly detector, or a natural language–to-SQL interface.

The core design philosophy is a **Hybrid Reasoning Engine**: **"SQL for strict math, LLM for medical reasoning."** Counting, aggregating, and detecting statistical outliers are handled by optimised Unity Catalog SQL functions — operations the LLM would otherwise perform unreliably. The LLM is reserved exclusively for what it excels at: interpreting structured results, synthesising clinical context, and producing coherent, human-readable responses grounded in the SQL output.

### Technology Stack

| Layer                             | Technology                                                                               |
| --------------------------------- | ---------------------------------------------------------------------------------------- |
| **Agent Framework**               | **LangGraph** `StateGraph` — explicit node/edge routing, tool postprocessor node         |
| **LLM — Agent Reasoning**         | Databricks Model Serving · configurable via `LLM_ENDPOINT`                               |
| **Tool — Geospatial**             | Unity Catalog SQL function (`find_facilities_nearby`) · `ST_DistanceSpheroid` (WGS84)    |
| **Tool — Semantic Search**        | **Databricks Vector Search** · HYBRID mode + `databricks_reranker` on `fact_text`        |
| **Tool — Anomaly / Gap Analysis** | Unity Catalog SQL function (`analyze_medical_query`) · 3-branch RLIKE dispatch           |
| **Tool — Quantitative SQL**       | **Databricks Genie** (`genie_chat_tool`) · natural language → SQL on `regional_insights` |
| **Geocoding**                     | **LocationIQ API** · resolves plain location names to lat/lon before SQL execution       |
| **API Server**                    | **FastAPI** · modular routers (`api/routes/agent.py`, `api/routes/map.py`)               |
| **Tracing / Observability**       | **MLflow Tracing** · named spans per pipeline type (geo_coldspot, geo_semantic)          |
| **Structured Output**             | **Pydantic v2** · request/response schemas in `api/schemas/`                             |

---

### 1. Agent Architecture & Query Routing

Queries are intercepted by our LangChain agent (`agent.py`) and routed dynamically based on natural language intent:

1. **Cold-Spot Queries (`IS_COLDSPOT`)** — _highest priority_:
   - Example: "Where are the largest geographic cold spots where a critical procedure is absent within 50km?"
   - Router: **`geospatial_query_tool` → `vector_search_tool`** (Cold-Spot Pipeline)
   - Logic: Identifies geographic areas where life-saving procedures are _absent_. The postprocessor computes the set difference (geo facilities − vector search matches) to find uncovered facilities, then groups them into a regional gap report.
2. **Quantitative / Ad-hoc Queries (`IS_QUANTITATIVE`)**:
   - Example: "How many hospitals are in Accra?"
   - Router: **Databricks Genie (`genie_chat_tool`)**
   - Logic: Translates natural language to SQL on the fly to count/aggregate clean schema data.
3. **Structural / Analytic Queries (`IS_ANALYTIC`)**:
   - Example: "Which facilities have suspicious overclaims for surgery?"
   - Router: **Medical Agent Engine (`medical_agent_tool`)** or **Geospatial Engine**
   - Logic: Executes highly optimized, pre-computed pure-SQL branches for anomaly detection and geospatial clustering within Unity Catalog.
4. **Semantic / Knowledge Queries (`IS_SEMANTIC`)**:
   - Example: "Which facilities provide cardiac surgery?"
   - Router: **Vector Search (`vector_search_tool`)**
   - Logic: Performs semantic similarity search over pre-generated facility facts stored in the `facility_facts` table.
5. **Geospatial Queries (`IS_GEOSPATIAL`)**:
   - Example: "Find clinics within 30 km of Kumasi."
   - Router: **Geospatial Engine (`geospatial_query_tool`)**
   - Logic: Geocodes the location name via LocationIQ, then executes ST_DistanceSpheroid calculations on the WGS84 spheroid via a Unity Catalog SQL function.
6. **Out-of-Scope**:
   - Logic: Blocked natively via system prompt refusal.

#### Priority Routing Table

| Priority | Classification              | Tools Used                                     |
| -------- | --------------------------- | ---------------------------------------------- |
| 1 ★      | IS_COLDSPOT                 | `geospatial_query_tool` → `vector_search_tool` |
| 2 ★      | IS_GEOSPATIAL + IS_SEMANTIC | `geospatial_query_tool` → `vector_search_tool` |
| 3 ★      | IS_GEOSPATIAL + IS_ANALYTIC | `geospatial_query_tool` → `medical_agent_tool` |
| 4        | IS_GEOSPATIAL only          | `geospatial_query_tool`                        |
| 5        | IS_ANALYTIC (any combo)     | `medical_agent_tool`                           |
| 6        | IS_SEMANTIC only            | `vector_search_tool`                           |
| 7        | IS_QUANTITATIVE only        | `genie_chat_tool`                              |

#### Shared Scope Filters

Both `setup_uc_function.sql` and `setup_geospatial.sql` accept the following optional attribute filters. All are `STRING`, all are optional, and `NULL` means no filter applied.

| Filter              | Accepted Values                                                                                   |
| ------------------- | ------------------------------------------------------------------------------------------------- |
| `facility_type`     | `'hospital'` \| `'clinic'` \| `'dentist'` \| `'farmacy'` \| `'doctor'`                            |
| `operator_type`     | `'private'` \| `'public'`                                                                         |
| `organization_type` | `'facility'` \| `'ngo'`                                                                           |
| `affiliation_type`  | `'faith-tradition'` \| `'government'` \| `'community'` \| `'philanthropy-legacy'` \| `'academic'` |

---

### 2. The Medical Agent Engine (`setup_uc_function.sql`)

This pure-SQL Unity Catalog function (`med_atlas_ai.default.analyze_medical_query`) handles complex anomaly detection and gap analysis. It contains **3 distinct logic branches**. All branches operate exclusively on `facility_records` — no joins against the high-volume `facility_facts` table. Branch dispatch is driven by `RLIKE` keyword matching on the `query` string passed from the agent.

| Branch                             | Check                | Trigger Keywords                                                                                                                                     | Mechanism                                                                                                                                                                                                                                                                                             |
| ---------------------------------- | -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Unmet Needs (Regional Gaps)** | Service Availability | `unmet`, `gap`, `need`, `service gap`                                                                                                                | Uses `ARRAY_EXCEPT` to find globally known medical specialties definitively missing from a specific region. Returns `specialties_missing` as a pre-computed SQL list — these are confirmed gaps. Free-text procedures and equipment are returned as-is for LLM medical reasoning.                     |
| **2. Anomaly Flagging**            | Statistical Outliers | `outlier`, `anomal`, `flag`, `unusual`, `inconsisten`, `signal`                                                                                      | Applies 3-sigma checks on capacity and doctor counts against a global baseline. Returns `plain_reason` fields describing each outlier in natural language. Caps at 100 results ordered by deviation magnitude.                                                                                        |
| **3. Deep Validation**             | Consistency          | `deep valid`, `validate`, `consistency`, `verify claim`, `mismatch`, `feature mismatch`, `procedure count`, `equipment count`, `infrastr`, `capable` | Region-scoped. SQL exports full facility profiles (specialties, procedures, equipment as comma-separated strings) with a `completeness` tag. Requires `region`, `facility_id`, or `facility_name`. Python agent batches **20 facilities at a time** through the LLM for medical consistency analysis. |

#### Geospatial Payload Enrichment

All SQL branches explicitly return `facility_id`, `latitude`, and `longitude` so the frontend map can pan to and highlight any facility mentioned in the LLM response.

#### `medical_agent_tool` — Filters

In addition to the [Shared Scope Filters](#shared-scope-filters) above, this function accepts:

| Filter          | Description                                                              |
| --------------- | ------------------------------------------------------------------------ |
| `region`        | Restrict to a specific state/region (e.g., `'Greater Accra'`)            |
| `city`          | Restrict to a specific city                                              |
| `facility_id`   | Look up a single facility by UUID                                        |
| `facility_name` | Look up a single facility by name (case-insensitive)                     |
| `facility_ids`  | Comma-separated list of UUIDs — used by the deep-validation batch runner |

#### The Missing Data Philosophy (NULL vs Zero)

- **`missing_data`**: the field is `NULL` — data was never collected.
- **`true_zero`**: the field exists but has no entries — confirmed absence of capability.

Branches return a `data_coverage_summary` payload so the LLM states data-gap caveats honestly before listing findings.

---

### 3. The Geospatial Engine (`setup_geospatial.sql`)

Handles distance calculations and spatial clustering using `ST_DistanceSpheroid` (WGS84 spheroid) for geodesic accuracy. **The Python agent always geocodes the reference location name (e.g., "Accra") via the LocationIQ API** before invoking the UC function — raw lat/lon coordinates are never passed directly by the LLM.

> **Why ST_DistanceSpheroid over Haversine?**
> Standard formulas like Haversine calculate distance across a perfect sphere, which introduces an error rate of up to 0.5% (distorting distances by several kilometers over long routes). By utilizing Databricks's native `ST_DistanceSpheroid` traversing the WGS84 Reference Spheroid model, the engine accounts for the Earth's equatorial bulge to provide **sub-meter accuracy**, making it the gold standard for straight-line geospatial mapping.

#### `geospatial_query_tool` — Parameters

| Parameter                | Type   | Required | Description                                                                                                                                                                       |
| ------------------------ | ------ | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `reference_location`     | string | **Yes**  | Plain location name (e.g., `"Accra"`, `"Volta region"`). Geocoded automatically via LocationIQ — raw lat/lon must never be passed.                                                |
| `radius_km`              | float  | No       | Search radius in kilometres. Default: **50**.                                                                                                                                     |
| `scan_all_ghana_regions` | bool   | No       | Set `True` for global cold-spot analysis. Geocodes all 16 Ghana regional capitals and returns the deduplicated union of all facilities found.                                     |
| _(shared filters)_       | string | No       | `facility_type`, `operator_type`, `organization_type`, `affiliation_type` — see [Shared Scope Filters](#shared-scope-filters). Only pass when the user explicitly mentioned them. |

#### Geocoding Flow

The LLM never resolves coordinates itself. The full flow is:

```
LLM passes reference_location="Kumasi"
        │
        ▼
geospatial_query_tool (Python)
  → GET https://us1.locationiq.com/v1/search
        ?key=<LOCATION_IQ_ACCESS_TOKEN>
        &q=Kumasi, Ghana
        &format=json
  ← [{ "lat": "6.6885116", "lon": "-1.6243874", ... }]
        │
        ▼
Builds payload: { "ref_lat": 6.688, "ref_lon": -1.624, "radius_km": 50, ... }
        │
        ▼
UCFunctionToolkit.invoke(query_json=<payload>)
  → Databricks SQL: find_facilities_nearby(query_json)
        → ST_DistanceSpheroid(facility_point, ST_Point(ref_lon, ref_lat))
        → WHERE distance_m / 1000 <= radius_km
        → ORDER BY distance_m ASC LIMIT 100
  ← JSON of up to 100 facilities with distance_km per facility
```

For `scan_all_ghana_regions=True`, this geocoding call is repeated for each of the 16 Ghana regional capitals with a 500 ms delay between requests to stay within the LocationIQ rate limit (2 req/sec). Results are deduplicated by `facility_id` before being returned.

---

### 4. Modular API Architecture (`ai_agent/api/`)

The server has been refactored into a scalable, modular structure to support both the LLM Agent and the Frontend Map UI.

#### Directory Structure

- `api/main.py`: Entry point that assembles FastAPI routers and middleware.
- `api/routes/`:
  - `agent.py`: LLM orchestration endpoints (`/invoke`, `/tools`).
  - `map.py`: Backend logic for the Map UI (`/map/search`, `/map/metadata`).
- `api/schemas/`: Pydantic models for request/response validation.

---

### 5. Map UI Backend API (`/map/`)

These endpoints power the interactive map interface for Ghana's healthcare infrastructure.

#### `GET /map/metadata`

- **Purpose**: Populates frontend filters (dropdowns, multi-selects).
- **Source**: All data is loaded entirely from a static `location.json` file co-located with the route. No SQL warehouse query is executed for this endpoint — the response is instantaneous.
- **Content**: Returns `regions` (list), `cities_by_region` (map), `specialties` (list), `facility_types`, `operator_types`, `organization_types`, and `affiliation_types`.

#### `POST /map/search`

- **Purpose**: Returns facility markers and summary cards based on user filters.
- **Payload Example**:
  ```json
  {
    "region": "Greater Accra Region",
    "city": "Accra",
    "specialties": ["Cardiology", "Dentistry"],
    "facility_type": "hospital",
    "operator_type": "public",
    "affiliation_types": ["government"]
  }
  ```
- **Features**:
  - **Viewport Bounding Box Filtering**: If a `bbox` (`[min_lat, min_lon, max_lat, max_lon]`) is provided, the API uses SQL `BETWEEN` operators to rapidly filter facilities strictly to the user's current map camera view.
  - **Advanced Array Filtering**: Uses `ARRAYS_OVERLAP` in SQL to efficiently filter multi-value fields like specialties.
  - **Count**: Returns a `count` field for the "Results Found" UI counter.

#### `GET /map/facility/{identifier}`

- **Purpose**: Fetches the complete medical profile for a single facility by ID or by name.
- **Lookup Logic**: The `identifier` path parameter is matched against both `facility_id` (exact match) and `facility_name` (case-insensitive, with automatic whitespace normalization — multiple spaces are collapsed to a single space before comparison).
- **Example URLs**:
  - `GET /map/facility/fac-123-abc`
  - `GET /map/facility/Korle-Bu%20Teaching%20Hospital`

---

### 6. LLM Agent Endpoints

- **`POST /invoke`**: Primary endpoint for conversational AI interaction.
- **`GET /health`**: Returns system status and tool availability.
- **`GET /tools`**: Returns the JSON schema for all agentic tools.

---

### 7. Map-LLM Integration (Two-Way Sync)

The architecture is uniquely designed to support **Two-Way Synchronization** between the Map UI and the Conversational Agent:

1. **Map drives the LLM**: Operations performed on the map (like moving the bounding box or applying filters) can be injected into the LLM context.
2. **LLM drives the Map (Citation Sync)**:
   - When the agent uses `medical_agent_tool` or `geospatial_query_tool`, the SQL engine returns `facility_id`, `latitude`, and `longitude`.
   - The parsers in `agent.py` capture these exact coordinates and embed them into the structured `citations` array.
   - When the Frontend receives the `/invoke` streaming response, it parses these citations and automatically plots, pans to, or pulses the pins for any facility the LLM decided to talk about in its response.

---

## Part III — Frontend

[← Back to Part II — AI Agent Pipeline](#part-ii--ai-agent-pipeline)

The frontend is a **React 19 + TypeScript** single-page application built with **Vite**. It provides two primary interfaces: an interactive **Mapbox GL** map for visually exploring Ghana's healthcare facilities with filter controls, and a conversational **chat panel** that streams responses from the AI agent in real time.

### Technology Stack

| Layer | Technology |
|---|---|
| **Framework** | React 19 + TypeScript |
| **Build tool** | Vite |
| **Styling** | Tailwind CSS v4 |
| **Map** | Mapbox GL JS v3 — facility pins, viewport bounding box, citation sync |
| **State management** | Zustand |
| **Server state / caching** | TanStack Query v5 |
| **HTTP client** | Axios |
| **Markdown rendering** | react-markdown + remark-gfm (for agent responses) |
| **Icons** | Lucide React |

### Key Behaviours

- **Citation Sync** — when the agent references specific facilities, the map automatically pans to and highlights their pins
- **Viewport-scoped search** — the map's current bounding box is passed to `/map/search` so results always reflect what is visible on screen
- **Streaming responses** — agent replies are streamed token-by-token from `/invoke` and rendered progressively in the chat panel

