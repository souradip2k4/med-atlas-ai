# Med-Atlas-AI: Intelligent Document Processing (IDP) Pipeline

A production-grade Databricks-powered IDP pipeline that ingests raw healthcare facility data from Ghana, extracts structured information through an LLM chain, generates semantically rich fact texts optimised for **Vector Search (RAG)**, and produces multi-dimensional **regional analytics** designed for **Text-to-SQL** querying. The pipeline writes to three core Delta tables on Unity Catalog, enabling a downstream LangGraph AI agent to answer complex healthcare infrastructure questions.

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
├── .env                                    # Credentials & config (gitignored)
└── README.md                               # This file
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

| #   | `insight_category` | `insight_value`      | `facility_count` | `total_capacity` | `total_doctors` |
| --- | ------------------ | -------------------- | ---------------- | ---------------- | --------------- |
| 1   | `overview`         | `all_facilities`     | ✅ countDistinct | ✅ SUM           | ✅ SUM          |
| 2   | `operator`         | `public` / `private` | ✅ countDistinct | ✅ SUM           | ✅ SUM          |
| 3   | `facility_type`    | `clinic`, `hospital`, `farmacy`, `doctor`, `dentist` | ✅ countDistinct | ✅ SUM           | ✅ SUM          |
| 4   | `organization`     | `facility` / `ngo`   | ✅ countDistinct | ✅ SUM           | ✅ SUM          |
| 5   | `specialty`        | e.g. `"cardiology"`  | ✅ countDistinct | `null`\*         | `null`\*        |
| 6   | `affiliation`      | `faith-tradition`, `government`, `community`, `philanthropy-legacy`, `academic` | ✅ countDistinct | `null`\*         | `null`\*        |

> \*`total_capacity` and `total_doctors` are explicitly `NULL` for array-based dimensions (`specialty`, `affiliation`) to prevent statistical overcounting — a hospital's bed count must not be multiplied by its number of specialties or affiliations.

**Geographic dimensions:** Each aggregation row covers both state-level totals (`city IS NULL`) and city-level breakdowns (`city IS NOT NULL`), so Genie can answer both "how many facilities in Greater Accra?" and "how many facilities specifically in Accra city?" from the same table.

#### `regional_insights` Delta Table Schema

| Column             | Type    | Description                                                    |
| ------------------ | ------- | -------------------------------------------------------------- |
| `country`          | String  | Always `"Ghana"`                                               |
| `state`            | String  | Ghana region (grouping dimension)                              |
| `city`             | String  | City (null = state-level aggregate)                            |
| `insight_category` | String  | `"overview"`, `"operator"`, or `"specialty"`                   |
| `insight_value`    | String  | `"all_facilities"` / `"public"` / `"private"` / specialty name |
| `facility_count`   | Integer | Distinct facilities in this slice                              |
| `total_capacity`   | Integer | SUM of bed capacity (null for specialty rows)                  |
| `total_doctors`    | Integer | SUM of doctor counts (null for specialty rows)                 |

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

## Environment Variables

Create a `.env` file in the `IDP/` directory:

```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
DATABRICKS_SERVERLESS=true                  # OR set DATABRICKS_CLUSTER_ID
CATALOG=med_atlas_ai_v2
SCHEMA=default
CSV_PATH=Virtue Foundation Ghana v0.3 - Sheet1.csv
LLM_ENDPOINT=databricks-gpt-oss-120b
MAX_WORKERS=6                               # Parallel extraction threads
MAX_PROCESS_ROWS=797                        # Max unique rows per run (max = 797)
GEMINI_API_KEY=AIzaXXXXXXXXXXXXXXXXXXXXX   # Gemini location
LOCATION_IQ_ACCESS_TOKEN=pk.XXXXXXXX       # LocationIQ geocoding
```

---

## Setup & Usage

```bash
# 1. Create virtual environment
uv venv
source .venv/bin/activate

# 2. Install dependencies
uv pip install -r requirements.txt

# 3. Configure .env (see above)

# 4. Run Stage 1–4: Extract & persist facility_records
uv run facility_record_generator.py

# 5. Run Stage 5: Generate facility_facts for Vector Search
uv run populate_facts.py

# 6. Run Stage 6: Compute regional_insights for Text-to-SQL
uv run compute_regional_insights.py
```

Each script is **idempotent via full overwrite** — re-running regenerates its target Delta table from scratch. There is no checkpointing. To process more rows, increase `MAX_PROCESS_ROWS` in `.env` and re-run `facility_record_generator.py`.

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
