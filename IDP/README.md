# Med-Atlas-AI: Intelligent Document Processing (IDP) Pipeline

A production-grade Databricks-powered IDP pipeline that ingests raw healthcare facility data from Ghana, extracts structured information through a 4-step LLM chain, generates semantically rich fact texts optimised for **Vector Search (RAG)**, and produces multi-dimensional **regional analytics** designed for **Text-to-SQL** querying. The pipeline writes to three core Delta tables on Unity Catalog, enabling a downstream LangGraph AI agent to answer complex healthcare infrastructure questions.

---

## Architecture Overview

The pipeline processes data through 7 sequential stages. Each CSV row passes through LLM extraction, deterministic merging, hybrid fact generation, and regional aggregation.

```
CSV File (Ghana healthcare facilities)
  │
  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 1 — Database Initialisation                                     │
│  DatabricksDatabase connects via Databricks Connect.                   │
│  Registers schemas for: facility_records, facility_facts,              │
│  regional_insights.                                                    │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 2 — CSV Loading (loader.py)                                     │
│  Reads the raw CSV with Pandas into memory as List[Dict].              │
│  Normalises column names, replaces NULL tokens with Python None.       │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 3 — Checkpointing                                               │
│  Reads existing facility_ids from facility_records to skip             │
│  already-processed rows. Respects MAX_PROCESS_ROWS budget.             │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 4 — LLM Extraction Chain (extractor.py)                         │
│  Each row is passed through 4 sequential LLM calls:                    │
│    ① Organization Extraction → facility names, NGO names               │
│    ② Facility Fact Extraction → procedures, equipment, capabilities    │
│    ③ Medical Specialty Extraction → specialties list                   │
│    ④ Facility Structured Info → address, contact, capacity, etc.       │
│  Runs in parallel via ThreadPoolExecutor (configurable MAX_WORKERS).   │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 5 — Merge & Shape (merger.py)                                   │
│  Consolidates all 4 LLM outputs + original CSV row into a single      │
│  flat dict matching FACILITY_RECORDS_SCHEMA.                           │
│  Uses _first_non_null() for scalars, _merge_arrays() for lists.       │
│  Includes deterministic Ghana city→region lookup fallback.             │
│  Output → facility_records Delta table                                 │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 6 — Hybrid Fact Generation (fact_generator.py)                  │
│  Generates ≤5 semantically rich rows per facility:                     │
│    • 1 "summary" row (all scalar fields merged into a paragraph)       │
│    • 1 "procedure" row (all procedures comma-joined)                   │
│    • 1 "equipment" row (all equipment comma-joined)                    │
│    • 1 "capability" row (all capabilities comma-joined)                │
│    • 1 "specialty" row (all specialties comma-joined)                  │
│  Missing data → row is silently skipped (never "Unknown"/"null").      │
│  Output → facility_facts Delta table (for Vector Search / RAG)         │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 7 — Regional Insights Aggregation (main.py)                     │
│  PySpark groupBy aggregations across 6 dimensions:                     │
│    ① overview — total facilities, beds, doctors per region             │
│    ② operator — public vs private breakdown with bed/doctor counts     │
│    ③ specialty — facility count per specialty per region               │
│    ④ procedure — facility count per procedure per region               │
│    ⑤ equipment — facility count per equipment type per region          │
│    ⑥ capability — facility count per capability per region             │
│  Output → regional_insights Delta table (for Text-to-SQL / Genie)     │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
IDP/
├── config/                                 # LLM prompt definitions & Pydantic output models
│   ├── __init__.py
│   ├── organization_extraction.py          # OrganizationExtractionOutput model + system prompt
│   ├── free_form.py                        # FacilityFacts model + system prompt
│   ├── medical_specialties.py              # MedicalSpecialties model + system prompt
│   └── facility_and_ngo_fields.py          # Facility / NGO structured models + system prompt
│
├── pipeline/                               # Core processing pipeline
│   ├── __init__.py
│   ├── loader.py                           # CSV → List[Dict] via Pandas
│   ├── preprocessor.py                     # Row → synthesised text block for LLM input
│   ├── extractor.py                        # 4-step LLM extraction chain (ChatDatabricks)
│   ├── merger.py                           # Merges LLM outputs + CSV into facility_records
│   └── fact_generator.py                   # Generates fact_text rows for Vector Search
│
├── storage/                                # Database layer
│   ├── __init__.py
│   ├── database.py                         # DatabricksDatabase class (session + Delta I/O)
│   └── models.py                           # PySpark StructType schemas for all tables
│
├── main.py                                 # Pipeline orchestrator (all 7 stages)
├── requirements.txt                        # Python dependencies
├── .env                                    # Credentials & config (gitignored)
└── README.md                               # This file
```

---

## Detailed Pipeline Stages

### Stage 1 — Database Initialisation

**File:** `main.py` (lines 52–59), `storage/database.py`

The pipeline creates a `DatabricksDatabase` instance which lazily initialises a Spark session via **Databricks Connect**. It requires either `DATABRICKS_CLUSTER_ID` or `DATABRICKS_SERVERLESS=true` in `.env`. The session sets the default catalog and schema (e.g., `med_atlas_ai.default`).

Schema registration happens at startup for all three output tables, but actual table creation is deferred until the first write operation.

### Stage 2 — CSV Loading

**File:** `pipeline/loader.py`

Reads the raw CSV file using Pandas with `dtype=str` to preserve all data as strings. Performs two cleaning steps:

1. **Column name normalisation** — strips whitespace, replaces special characters with underscores, lowercases.
2. **NULL token replacement** — converts `"null"`, `"None"`, `"N/A"`, `""`, and `NaN` values into Python `None`.

Returns a `List[Dict[str, Any]]` where each dict is one CSV row.

### Stage 3 — Checkpointing

**File:** `main.py` (lines 69–110)

Before processing, the pipeline reads all existing `facility_id` values from the `facility_records` table. Any CSV row whose `unique_id` (or `pk_unique_id`) already exists in `facility_records` is skipped.

The `MAX_PROCESS_ROWS` environment variable caps the total number of rows the pipeline will ever process (across all runs combined). This prevents runaway LLM costs during development.

### Stage 4 — LLM Extraction Chain

**File:** `pipeline/extractor.py`, `pipeline/preprocessor.py`

**Step 4a: Text Synthesis (`preprocessor.py`)**
Each CSV row is first converted into a structured text block by `synthesize_row_text()`. It iterates over all columns (skipping identity columns like `pk_unique_id`), formats non-null values as `"Field Name: value"` lines, and parses JSON arrays into bulleted lists. This synthesised text becomes the input to all 4 LLM calls.

**Step 4b: 4-Step LLM Chain (`extractor.py`)**
Uses `ChatDatabricks` (LangChain integration) pointed at a Databricks Model Serving endpoint (default: `databricks-meta-llama-3-1-70b-instruct`).

| Step | Prompt Config | Pydantic Output Model | What It Extracts |
|------|---------------|----------------------|------------------|
| ① Organization Extraction | `organization_extraction.py` | `OrganizationExtractionOutput` | Facility names, NGO names, organisation description, mission statement |
| ② Facility Fact Extraction | `free_form.py` | `FacilityFacts` | Procedures, equipment, capabilities (free-form medical facts) |
| ③ Medical Specialty Extraction | `medical_specialties.py` | `MedicalSpecialties` | List of medical specialties |
| ④ Facility Structured Info | `facility_and_ngo_fields.py` | `Facility` | Address lines, city, state, country, phone numbers, email, websites, year established, capacity, number of doctors, volunteer status, affiliations, operator type, facility type |

Each LLM call appends a JSON enforcement suffix to ensure valid JSON output. Responses are stripped of markdown code fences and parsed through Pydantic `model_validate_json`. Failed validation logs a warning and returns `None` for that step (the merge stage gracefully handles missing steps).

Rows are processed in parallel using `ThreadPoolExecutor` with configurable `MAX_WORKERS` (default: 4).

### Stage 5 — Merge & Shape

**File:** `pipeline/merger.py`

The `merge_extraction_results()` function consolidates the outputs of all 4 LLM extraction steps plus the original CSV row into a single flat dictionary matching `FACILITY_RECORDS_SCHEMA`.

**Merging Strategy:**
- **Scalar fields** — uses `_first_non_null()` which returns the first non-None, non-empty value from a priority list. LLM extraction output is prioritised over raw CSV values.
- **Array fields** — uses `_merge_arrays()` which concatenates multiple lists and deduplicates entries (case-insensitive) while preserving insertion order.
- **State/Region inference** — includes a deterministic `_GHANA_CITY_REGION` lookup table (80+ cities mapped to their Ghana regions). If the LLM and CSV both fail to provide a region, the merger infers it from the city name.

**Identity mapping:**
The `facility_id` is set to the original CSV row's `unique_id` (or `pk_unique_id`). This allows the pipeline's checkpointing mechanism to match CSV rows against already-processed records.

### Stage 6 — Hybrid Fact Generation

**File:** `pipeline/fact_generator.py`

This is the core of the **Hybrid RAG Strategy**. Instead of generating dozens of paraphrased rows per facility (which causes "vector crowding" in top_k retrieval), the pipeline generates a maximum of **5 rows per facility**.

#### How fact_text is generated

**Location String (`loc_str`):**
Before generating any fact, the generator constructs a geographic context string from `city`, `state`, and `country`. Example: `" in Accra, Greater Accra Region, Ghana"`. If all three are null, the string is empty. This `loc_str` is injected into every fact_text to ensure the Vector Search can match geographic queries.

**Array Fact Rows (up to 4 rows):**

For each of the 4 array fields in `facility_records`, the generator creates exactly **one** consolidated sentence per fact_type. If the array is empty or null, the row is silently skipped.

| fact_type | Source Column in facility_records | Template | Example fact_text |
|-----------|----------------------------------|----------|-------------------|
| `procedure` | `procedures` (Array) | `"{facility}{location} provides the following medical procedures: {items}."` | `"1st Foundation Clinic in Accra, Greater Accra Region, Ghana provides the following medical procedures: bloodTransfusion, woundSuturing."` |
| `equipment` | `equipment` (Array) | `"{facility}{location} is equipped with: {items}."` | `"WAAF in Takoradi, Western Region, Ghana is equipped with: ultrasoundMachine, xRayMachine."` |
| `capability` | `capabilities` (Array) | `"{facility}{location} has the following clinical capabilities: {items}."` | `"3E Medical Center in Accra, Greater Accra Region, Ghana has the following clinical capabilities: Offers 24-hour medical services, Offers inpatient services."` |
| `specialty` | `specialties` (Array) | `"{facility}{location} offers specialty care in: {items}."` | `"Ahmadiyya Hospital in Kumasi, Ashanti Region, Ghana offers specialty care in: internalMedicine, familyMedicine."` |

All items in the array are comma-joined into a single string. Empty strings and whitespace-only items are stripped out before joining.

**Summary Fact Row (exactly 1 row):**

All scalar fields are consolidated into a single rich paragraph. The summary is constructed by conditionally appending sentence fragments (only if the source data exists). The following fields from `facility_records` contribute to the summary:

| Field | How it appears in the summary |
|-------|-------------------------------|
| `facility_name` | Always present — leads the sentence |
| `operator_type` | `"privately operated"` / `"publicly operated"` prefix |
| `facility_type` | Type label (e.g., `"clinic"`, `"hospital"`) |
| `city`, `state`, `country` | Geographic location via `loc_str` |
| `address_line1`, `address_line2`, `address_line3` | `"It is physically located at [full address]."` — only non-null lines are concatenated |
| `capacity` | `"an inpatient capacity of X beds."` |
| `year_established` | `"Established in YYYY."` |
| `accepts_volunteers` | `"It actively accepts clinical volunteers."` (only if `True`) |
| `affiliation_types` | `"It is affiliated with the following types: X, Y."` |
| `mission_statement` | `"Its mission statement is: ..."` |
| `description` | `"Description: ..."` |

**Example Summary fact_text:**
```
"WAAF is a privately operated clinic in Takoradi, Western Region, Ghana.
 It is physically located at 123 Main Street, Suite 4B.
 It has an inpatient capacity of 40 beds.
 Established in 2005. It actively accepts clinical volunteers.
 Its mission statement is: To provide quality healthcare to the people of Western Ghana."
```

**Missing Data Handling:**
If a field is `None`, empty, or missing, the corresponding sentence fragment is silently omitted. The word "Unknown" or "null" is **never** inserted into any fact_text. If an entire array is empty, no row is generated for that fact_type. This prevents the Vector DB from returning false positives on pattern-matched "unknown" strings.

### Stage 7 — Regional Insights Aggregation

**File:** `main.py` (function `_compute_regional_insights()`)

This stage builds a multi-dimensional analytics table using PySpark aggregations on `facility_records`. The table is designed exclusively for **Text-to-SQL** querying (via Databricks Genie or a LangGraph SQL tool) — it is NOT injected into the Vector DB.

**Six aggregation dimensions:**

| # | insight_category | insight_value | What is grouped | facility_count | total_beds |
|---|-----------------|---------------|-----------------|----------------|------------|
| 1 | `overview` | `all_facilities` | All facilities in a region | ✅ countDistinct | ✅ SUM(capacity) |
| 2 | `operator` | e.g. `"public"`, `"private"` | Facilities by operator_type | ✅ countDistinct | ✅ SUM(capacity) |
| 3 | `specialty` | e.g. `"cardiology"` | Facilities by exploded specialty | ✅ countDistinct | NULL* |
| 4 | `procedure` | e.g. `"openHeartSurgery"` | Facilities by exploded procedure | ✅ countDistinct | NULL* |
| 5 | `equipment` | e.g. `"mriMachine"` | Facilities by exploded equipment | ✅ countDistinct | NULL* |
| 6 | `capability` | e.g. `"24-hour services"` | Facilities by exploded capability | ✅ countDistinct | NULL* |

*\*`total_beds` is explicitly set to `NULL` for array-based dimensions to prevent statistical overcounting. A hospital with 50 beds and 3 specialties would otherwise contribute 150 phantom beds to the totals.*

**Array Explosion Logic:**
For array columns (specialties, procedures, equipment, capabilities), PySpark's `explode_outer()` is used to create one row per array item, followed by `groupBy(country, state, city, item)` and `countDistinct(facility_id)`. This guarantees that each facility is counted exactly once per specialty/procedure/equipment/capability even if duplicate entries exist in the source data.

---

## Output Delta Tables

### 1. `facility_records` — The Master Structured Table

The single source of truth for all facility data. Each row represents one healthcare facility.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `facility_id` | String | No | Primary key — set to the original CSV row's `unique_id` for checkpointing |
| `facility_name` | String | No | Name of the healthcare facility |
| `organization_type` | String | No | Type of organisation (`"facility"`, `"ngo"`, etc.) |
| `specialties` | Array[String] | Yes | Medical specialties offered (e.g., `["cardiology", "oncology"]`) |
| `procedures` | Array[String] | Yes | Medical procedures performed (e.g., `["bloodTransfusion"]`) |
| `equipment` | Array[String] | Yes | Medical equipment available (e.g., `["ultrasoundMachine"]`) |
| `capabilities` | Array[String] | Yes | Clinical capabilities (e.g., `["24-hour services"]`) |
| `address_line1` | String | Yes | Street address line 1 |
| `address_line2` | String | Yes | Street address line 2 |
| `address_line3` | String | Yes | Street address line 3 |
| `city` | String | Yes | City name (e.g., `"Accra"`) |
| `state` | String | Yes | State or region (e.g., `"Greater Accra Region"`) |
| `country` | String | Yes | Country name (e.g., `"Ghana"`) |
| `country_code` | String | Yes | ISO country code (e.g., `"GH"`) |
| `phone_numbers` | Array[String] | Yes | Contact phone numbers |
| `email` | String | Yes | Primary email address |
| `websites` | Array[String] | Yes | Associated websites |
| `social_links` | Map[String, String] | Yes | Dictionary mapping platforms to URLs (e.g., `{"facebookLink": "url"}`) |
| `officialWebsite` | String | Yes | Official website URL |
| `year_established` | Integer | Yes | Year the facility was established |
| `accepts_volunteers` | Boolean | Yes | Whether the facility accepts clinical volunteers |
| `capacity` | Integer | Yes | Inpatient bed capacity |
| `description` | String | Yes | Facility description text |
| `mission_statement` | String | Yes | Organisation mission statement |
| `affiliation_types` | Array[String] | Yes | Affiliation type IDs |
| `operator_type` | String | Yes | Operator type (`"public"`, `"private"`, etc.) |
| `facility_type` | String | Yes | Facility classification (`"clinic"`, `"hospital"`, etc.) |
| `created_at` | Timestamp | Yes | Record creation timestamp (UTC) |
| `updated_at` | Timestamp | Yes | Record last update timestamp (UTC) |

### 2. `facility_facts` — Vector Search / RAG Table

Optimised for embedding and semantic retrieval. Each row contains a single, self-contained natural language sentence that can be independently embedded and searched. A downstream embedding step generates vector representations of the `fact_text` column, which are stored in a Vector Search index.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `fact_id` | String | No | Unique identifier (UUID) for this fact row |
| `facility_id` | String | No | Foreign key → `facility_records.facility_id` |
| `fact_text` | String | No | The natural language sentence to be embedded and searched |
| `fact_type` | String | No | Category tag: `"summary"`, `"procedure"`, `"equipment"`, `"capability"`, or `"specialty"` |
| `source_text` | String | Yes | The raw source data that was used to generate this fact_text |

**Row count per facility:** Maximum 5 rows (1 summary + up to 4 array types). If a facility has no procedures, no procedure row is generated. Minimum is 1 row (the summary).

**Why this structure matters for Vector Search:**
- The `fact_type` column enables **metadata filtering** in the Vector Search index. An agent can search for `query="MRI machine" + filter={"fact_type": "equipment"}` to guarantee zero noise from description or procedure texts.
- Each `fact_text` is geographically contextualised (city, state, country are embedded in the sentence), enabling geographic similarity matching without structured SQL.

### 3. `regional_insights` — Text-to-SQL / BI Analytics Table

A pre-aggregated multi-dimensional analytics table designed for precise quantitative queries via Text-to-SQL. This table is **NOT** used for vector search.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `country` | String | Yes | Country (grouping dimension) |
| `state` | String | Yes | State/Region (grouping dimension) |
| `city` | String | Yes | City (grouping dimension) |
| `insight_category` | String | No | Dimension type: `"overview"`, `"operator"`, `"specialty"`, `"procedure"`, `"equipment"`, `"capability"` |
| `insight_value` | String | No | Dimension value (e.g., `"all_facilities"`, `"cardiology"`, `"public"`, `"mriMachine"`) |
| `facility_count` | Integer | Yes | Number of distinct facilities matching this dimension |
| `total_beds` | Integer | Yes | Sum of bed capacity (only for `overview` and `operator` categories; NULL otherwise) |
| `contributing_facility_ids` | Array[String] | Yes | List of facility_ids contributing to this aggregate |

**Example SQL queries this table enables:**
```sql
-- "How many total hospital beds are in Greater Accra?"
SELECT total_beds FROM regional_insights
WHERE state = 'Greater Accra Region' AND insight_category = 'overview';

-- "How many facilities in Western Region perform blood transfusions?"
SELECT facility_count FROM regional_insights
WHERE state = 'Western Region' AND insight_category = 'procedure'
  AND insight_value = 'bloodTransfusion';

-- "Compare public vs private hospital capacity in Kumasi"
SELECT insight_value, facility_count, total_beds
FROM regional_insights
WHERE city = 'Kumasi' AND insight_category = 'operator';
```

---

## Dual Retrieval Architecture

The pipeline produces data for **two distinct retrieval strategies**, used by the downstream LangGraph agent:

| Query Type | Retrieval Method | Table Used | Example Query |
|-----------|-----------------|-----------|---------------|
| **Semantic / Qualitative** | Vector Search (RAG) | `facility_facts` | *"Find clinics near Accra that do cardiac surgery"* |
| **Quantitative / Statistical** | Text-to-SQL (Genie) | `regional_insights` | *"How many hospitals in Western Region have MRI machines?"* |
| **Facility Lookup** | Direct SQL or RAG | `facility_records` / `facility_facts` | *"What services does Korle Bu Teaching Hospital offer?"* |
| **Anomaly Detection** | Agent-side LLM reasoning | Both tables | *"Which clinics claim surgeries but have no beds?"* → Agent cross-references procedure facts against summary facts |

---

## Environment Variables

Create a `.env` file in the `IDP/` directory:

```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
DATABRICKS_CLUSTER_ID=your-cluster-id          # OR set DATABRICKS_SERVERLESS=true
CATALOG=med_atlas_ai
SCHEMA=default
CSV_PATH=Virtue Foundation Ghana v0.3 - Sheet1.csv
LLM_ENDPOINT=databricks-meta-llama-3-1-70b-instruct
MAX_WORKERS=4                                   # Parallel extraction threads
MAX_PROCESS_ROWS=100                            # Cap total processed rows (across all runs)
```

---

## Setup & Usage

```bash
# 1. Create virtual environment
python -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure .env with your Databricks credentials (see above)

# 4. Run the full pipeline
python main.py
```

The pipeline is **idempotent** — running it multiple times will only process new rows that haven't been checkpointed. To reprocess all data, drop the `facility_records` table in Databricks first.

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Hybrid fact generation (≤5 rows/facility)** | Prevents "vector crowding" where top_k=10 returns 10 rows about the same hospital. Each row is semantically dense and independently useful. |
| **No "Unknown" or "null" in fact_text** | If data is missing, the row is silently skipped. This prevents the Vector DB from returning false-positive matches on the word "unknown". |
| **Geographic context in every fact_text** | City, state, and country are embedded directly in the sentence (e.g., `"in Accra, Greater Accra Region, Ghana"`). This enables geographic similarity matching without structured filters. |
| **regional_insights uses Text-to-SQL, not RAG** | Vector Search is mathematically unreliable for counting, ranking, and aggregation. Text-to-SQL gives exact, provably correct quantitative answers. |
| **Overcounting prevention** | `total_beds` is NULL for array-based insight categories (specialty, procedure, equipment, capability) to prevent a single hospital's metrics from being multiplied across its specialties. |
| **facility_id = CSV unique_id** | Eliminates the need for a separate `source_row_id` column. Enables idempotent checkpointing by comparing CSV row IDs against existing `facility_id`s. |
| **Deterministic Ghana region inference** | A 80+ entry city→region lookup table in `merger.py` ensures consistent region assignment even when the LLM fails to extract the state/region. |
| **One LLM call per row, no cross-row batching** | Prevents cross-contamination where the LLM conflates data from different facilities within a single prompt. |
| **Schema evolution via mergeSchema** | Delta table writes use `.option("mergeSchema", "true")` to gracefully handle schema additions across pipeline iterations without requiring table drops. |
