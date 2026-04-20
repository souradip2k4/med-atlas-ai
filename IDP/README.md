# Med-Atlas-AI: Intelligent Document Processing (IDP) Pipeline

A production-grade Databricks-powered IDP pipeline that ingests raw healthcare facility data from Ghana, extracts structured information through an LLM chain, generates semantically rich fact texts optimised for **Vector Search (RAG)**, and produces multi-dimensional **regional analytics** designed for **Text-to-SQL** querying. The pipeline writes to three core Delta tables on Unity Catalog, enabling a downstream LangGraph AI agent to answer complex healthcare infrastructure questions.

---

## Architecture Overview

The pipeline processes data through 6 sequential stages. Each CSV row passes through deduplication, LLM extraction, a 5-step deterministic merge (including Gemini-based location inference and LocationIQ geocoding), hybrid fact generation, and regional aggregation.

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
│  Each row is passed through a single LLM call:                           │
│    ① Facility Fact Extraction → procedures, equipment,                   │
│       capabilities, specialties, description, addresses,                 │
│       and contact fields — free-form medical facts.                      │
│  Runs in parallel via ThreadPoolExecutor (configurable MAX_WORKERS).     │
│  Capability arrays pass through deterministic _GARBAGE_KEYWORDS          │
│  filtering (case-insensitive fuzzy match) to strip directory/contact     │
│  noise BEFORE any downstream use.                                        │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 4 — Merge & Shape (merger.py)                                     │
│  Consolidates the LLM output + original CSV row into a single flat dict  │
│  matching FACILITY_RECORDS_SCHEMA via a 5-step location cascade:         │
│    1. Direct CSV values (city, state).                                   │
│    2. Deterministic _GHANA_CITY_REGION dictionary lookup (100+ cities).  │
│    3. Gemini API — Case 1: city known, region missing.                   │
│    4. Gemini API — Case 2: both city AND region missing.                 │
│    5. LocationIQ geocoder — resolves lat/lon from the full address.      │
│  Gemini calls are controlled by a StrictThrottle (14 RPM) shared across  │
│  all workers. LocationIQ is guarded by a global threading.Lock enforcing │
│  a 2-requests/second rate limit.                                         │
│  _clean_array() applies _GARBAGE_KEYWORDS filter on all array fields.    │
│  Output → facility_records Delta table (single overwrite at run end)     │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 5 — Hybrid Fact Generation (facility_fact_generator.py)           │
│  Generates ≤6 semantically rich rows per facility:                       │
│    • 1 "summary"     row  — core identity: name, type, location,         │
│                             org_type, affiliation (NO description/mission)│
│    • 1 "description" row  — narrative: description + mission_statement   │
│    • 1 "procedure"   row  — all procedures comma-joined                  │
│    • 1 "equipment"   row  — all equipment comma-joined                   │
│    • 1 "capability"  row  — all capabilities comma-joined                │
│    • 1 "specialty"   row  — all specialties comma-joined                 │
│  Missing data → row is silently skipped (never "Unknown"/"null").        │
│  Output → facility_facts Delta table (single overwrite at run end)       │
└─────────────┬────────────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 6 — Regional Insights Aggregation (compute_regional_insights.py)  │
│  PySpark groupBy aggregations across 3 dimensions:                       │
│    ① overview  — total facilities, beds, doctors per region/city         │
│    ② operator  — public vs private breakdown with bed/doctor counts      │
│    ③ specialty — facility count per specialty per region/city            │
│  Output → regional_insights Delta table (for Text-to-SQL / Genie)        │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
IDP/
├── config/                                 # LLM prompt definitions & Pydantic output models
│   ├── __init__.py
│   ├── free_form.py                        # FacilityFacts model + system prompt (primary LLM step)
│   └── medical_specialties.py             # MedicalSpecialties model (used for reference)
│
├── pipeline/                               # Core processing pipeline
│   ├── __init__.py
│   ├── loader.py                           # CSV → List[Dict] via Pandas
│   ├── deduplicator.py                     # pk_unique_id deduplication + row merging
│   ├── preprocessor.py                     # Row → synthesised text block for LLM input
│   ├── extractor.py                        # LLM extraction chain + _GARBAGE_KEYWORDS filter
│   ├── merger.py                           # Merges LLM outputs + CSV into facility_records
│   ├── geocoder.py                         # LocationIQ geocoder (rate-limited, 2 req/s)
│   ├── location_resolver.py               # Gemini-powered city/region inference (14 RPM)
│   └── facility_fact_generator.py          # Generates fact_text rows for Vector Search
│
├── storage/                                # Database layer
│   ├── __init__.py
│   ├── database.py                         # DatabricksDatabase class (session + Delta I/O)
│   └── models.py                           # PySpark StructType schemas for all tables
│
├── facility_record_generator.py            # Pipeline orchestrator (Stages 1–4)
├── populate_facts.py                       # Standalone script — runs Stage 5 on all records
├── compute_regional_insights.py            # Standalone script — runs Stage 6
├── .env                                    # Credentials & config (gitignored)
└── README.md                               # This file
```

---

## Detailed Pipeline Stages

### Stage 1 — Database Initialisation

**File:** `facility_record_generator.py`, `storage/database.py`

Creates a `DatabricksDatabase` instance which lazily initialises a Spark session via **Databricks Connect**. Requires either `DATABRICKS_CLUSTER_ID` or `DATABRICKS_SERVERLESS=true` in `.env`. The session sets the default catalog and schema from `CATALOG` and `SCHEMA` env vars (e.g., `med_atlas_ai_v2.default`).

Schema registration happens at startup for all three output tables, but actual table creation is deferred until first write.

### Stage 2 — CSV Loading

**File:** `pipeline/loader.py`

Reads the raw CSV file using Pandas with `dtype=str` to preserve all data as strings. Performs two cleaning steps:

1. **Column name normalisation** — strips whitespace, replaces special characters with underscores, lowercases.
2. **NULL token replacement** — converts `"null"`, `"None"`, `"N/A"`, `""`, and `NaN` values into Python `None`.

Returns a `List[Dict[str, Any]]` where each dict is one raw CSV row.

### Stage 2b — Deduplication

**File:** `pipeline/deduplicator.py`

The raw CSV contains **987 rows** representing 797 unique facilities. The extra 190 rows are duplicates — the same facility scraped from multiple web sources, resulting in multiple rows sharing the same `pk_unique_id` but with complementary non-null data.

**How it works:** Rows are grouped by `pk_unique_id`. For each group, a single merged row is produced by taking, for each column, the first non-null value across duplicate rows. This ensures data captured from any source page for the same facility is preserved.

After deduplication, `MAX_PROCESS_ROWS` (from `.env`) slices the 797 unique records to limit LLM cost per run.

### Stage 3 — LLM Extraction Chain

**File:** `pipeline/extractor.py`, `pipeline/preprocessor.py`

**Step 3a: Text Synthesis (`preprocessor.py`)**
Each CSV row is converted into a structured text block by `synthesize_row_text()`. It iterates over all columns (skipping identity columns), formats non-null values as `"Field Name: value"` lines, and parses JSON arrays into bulleted lists.

**Step 3b: LLM Extraction (`extractor.py`)**
Uses `ChatDatabricks` (LangChain integration) pointed at a Databricks Model Serving endpoint (configurable via `LLM_ENDPOINT`).

| Step | Prompt Config | Pydantic Output Model | What It Extracts |
|------|--------------|----------------------|-----------------|
| ① Facility Fact Extraction | `free_form.py` | `FacilityFacts` | Procedures, equipment, capabilities, specialties, cleaned facility name, description, mission statement |

Responses are stripped of markdown fences and parsed via Pydantic `model_validate_json`. Failed validation logs a warning and returns `None`.

**Capability Garbage Filtering:** After extraction, all array fields (especially `capabilities`) are passed through `_clean_array()` using the shared `_GARBAGE_KEYWORDS` list from `extractor.py`. This is a case-insensitive fuzzy-match filter that strips strings originating from directory listings, contact information, web scraper artifacts, and similar non-clinical noise (e.g., strings containing `"telephone"`, `"ghanaYello"`, `"listed in"`, `"registered with"`). If all items in an array are filtered, the field is stored as `null` — never as an empty list or raw garbage.

Rows are processed in parallel using `ThreadPoolExecutor` with configurable `MAX_WORKERS`.

### Stage 4 — Merge & Shape

**File:** `pipeline/merger.py`

The `merge_extraction_results()` function consolidates the LLM extraction output plus the original CSV row into a single flat dictionary matching `FACILITY_RECORDS_SCHEMA`.

**Merging Strategy:**

- **Scalar fields** — uses `_first_non_null()` which returns the first non-`None`, non-empty value from a priority list.
- **Array fields** — uses `_merge_arrays()` which concatenates multiple lists and deduplicates entries (case-insensitive) while preserving insertion order. All arrays additionally pass through `_clean_array()` as a safety net.

#### 5-Step Location Resolution Cascade

This is the most sophisticated part of the merge step. City and region (state) are resolved via a strict priority cascade:

**Step 1 — Direct CSV values:** Take city and state directly from the deduplicated CSV row.

**Step 2 — Dictionary lookup:** If state is missing but city is present, attempt an exact then substring match against `_GHANA_CITY_REGION` — a hardcoded mapping of 100+ Ghanaian city names to their regions.

**Step 3 — Gemini Case 1 (city known, region missing):** If the dictionary lookup also misses, call the `GeminiLocationResolver` with the known city name. Gemini infers the region using a full Ghana region→cities reference dictionary embedded in the prompt. It must return an exact match against the 16 valid Ghana region names.

**Step 4 — Gemini Case 2 (both city AND region missing):** If both city and state are absent, call Gemini with all available address lines. Gemini infers both city and region from facility name semantic cues and address fragments.

**Step 5 — LocationIQ Geocoder:** After city and state are fully resolved by the steps above, call `FacilityGeocoder` (LocationIQ API) to convert the full address string into `latitude` and `longitude`. It tries progressively degraded address queries until a result is found, or stores `null` if all queries fail.

**Rate Limiting:**
- **Gemini:** A module-level `StrictThrottle(14)` singleton enforces exactly one Gemini call every `60/14 ≈ 4.28 seconds` across all concurrent workers. The lock is acquired before each call, so workers queue single-file rather than bursting. This prevents Google's 15 RPM rolling-window quota from being exceeded.
- **LocationIQ:** A global `threading.Lock` with `time.sleep(0.5)` enforces a maximum of 2 requests/second across all workers on the free tier.

### Stage 5 — Hybrid Fact Generation

**File:** `pipeline/facility_fact_generator.py` | **Run via:** `populate_facts.py`

This is the core of the **Hybrid RAG strategy**. The pipeline generates a maximum of **6 rows per facility** to prevent "vector crowding" in top-k retrieval.

#### Location String (`loc_str`)

Before generating any fact, a geographic context string is built from `city`, `state`, and `country`. Example: `" in Accra, Greater Accra, Ghana"`. This is injected into every `fact_text` to enable geographic similarity matching without structured filters.

#### Fact Types Generated

| `fact_type`   | Source Fields                                  | Content                                                                                     |
|---------------|------------------------------------------------|---------------------------------------------------------------------------------------------|
| `summary`     | `operator_type`, `facility_type`, `org_type`, `city`, `state`, `country`, `affiliation_types` | Identity-focused sentence: who, what type, where, affiliation. **Does NOT include description or mission.** |
| `description` | `description`, `mission_statement`             | Narrative sentence: background, purpose, and mission of the facility.                       |
| `procedure`   | `procedures` (Array)                            | `"{facility}{location} provides the following medical procedures: {items}."`                |
| `equipment`   | `equipment` (Array)                             | `"{facility}{location} is equipped with: {items}."`                                         |
| `capability`  | `capabilities` (Array)                          | `"{facility}{location} has the following clinical capabilities: {items}."`                  |
| `specialty`   | `specialties` (Array)                           | `"{facility}{location} offers specialty care in: {items}."`                                 |

**Why `summary` and `description` are separate:** The `summary` fact is optimised purely for identity/type/affiliation matching ("find all private clinics in Ashanti"). The `description` fact is optimised for narrative/mission matching ("find facilities focused on maternal care"). Keeping them separate means the vector index retrieves the right signal for each query intent without noise from the other.

**Missing Data Handling:** If a field is `None` or empty, the corresponding row is silently skipped. The words "Unknown" and "null" are **never** inserted into any `fact_text`. If `description` and `mission_statement` are both null, no `description` row is generated.

#### Execution Model

`populate_facts.py` runs Stage 5 as a standalone script. It reads **all** rows from `facility_records` into memory, generates all facts, then performs a **single overwrite** to `facility_facts` at the end. There is no checkpointing or batched intermediate writes — this guarantees the final table always reflects the complete output.

### Stage 6 — Regional Insights Aggregation

**File:** `compute_regional_insights.py`

Builds a multi-dimensional analytics table using PySpark aggregations on `facility_records`. Designed exclusively for **Text-to-SQL** (via Databricks Genie) — **NOT** injected into the Vector DB.

**Three aggregation dimensions:**

| # | `insight_category` | `insight_value` | What is grouped | `facility_count` | `total_capacity` | `total_doctors` |
|---|--------------------|-----------------|-----------------|-----------------|-----------------|-----------------|
| 1 | `overview` | `all_facilities` | All facilities per region/city | ✅ countDistinct | ✅ SUM | ✅ SUM |
| 2 | `operator` | `public` / `private` | Facilities by operator_type | ✅ countDistinct | ✅ SUM | ✅ SUM |
| 3 | `specialty` | e.g. `"cardiology"` | Facilities per specialty | ✅ countDistinct | `null`* | `null`* |

*`total_capacity` and `total_doctors` are explicitly `NULL` for the `specialty` dimension to prevent statistical overcounting (a hospital with 3 specialties would otherwise contribute its bed count × 3).

**Geographic dimensions:** Each aggregation row covers both state-level totals (when `city IS NULL`) and city-level breakdowns (when `city IS NOT NULL`). This allows Genie to answer both "how many facilities in Greater Accra?" and "how many facilities in Accra city?" accurately from the same table.

---

## Output Delta Tables

### 1. `facility_records` — Master Structured Table

Each row is one unique healthcare facility. Written by `facility_record_generator.py` as a **full overwrite** at the end of each run — all rows processed in that run are written simultaneously.

| Column | Type | Description |
|--------|------|-------------|
| `facility_id` | String (PK) | Set from CSV `unique_id` / `pk_unique_id` |
| `facility_name` | String | Official name of the facility |
| `organization_type` | String | `"facility"` or `"ngo"` |
| `specialties` | Array[String] | Medical specialties offered |
| `procedures` | Array[String] | Medical procedures performed |
| `equipment` | Array[String] | Medical equipment available |
| `capabilities` | Array[String] | Clinical capabilities (garbage-filtered) |
| `address_line1/2/3` | String | Street address components |
| `city` | String | City/town |
| `state` | String | Ghana region (e.g., `"Greater Accra"`) |
| `country` | String | Always `"Ghana"` |
| `country_code` | String | ISO code (e.g., `"GH"`) |
| `latitude` | Double | Geocoded latitude (null if unresolvable) |
| `longitude` | Double | Geocoded longitude (null if unresolvable) |
| `phone_numbers` | Array[String] | Contact numbers |
| `email` | String | Contact email |
| `websites` | Array[String] | Associated URLs |
| `social_links` | Map[String,String] | Platform → URL mapping |
| `officialWebsite` | String | Primary homepage |
| `year_established` | Integer | Year founded |
| `accepts_volunteers` | Boolean | Accepts volunteers |
| `capacity` | Integer | Bed capacity |
| `no_doctors` | Integer | Doctor count |
| `description` | String | Narrative description of the facility |
| `mission_statement` | String | Official mission statement |
| `affiliation_types` | Array[String] | `philanthropy-legacy`, `academic`, `faith-tradition`, `government`, `community` |
| `operator_type` | String | `"public"` or `"private"` |
| `facility_type` | String | `"clinic"`, `"hospital"`, `"farmacy"`, `"doctor"`, `"dentist"` |
| `created_at` | Timestamp | Record creation time (UTC) |
| `updated_at` | Timestamp | Record last update time (UTC) |

### 2. `facility_facts` — Vector Search / RAG Table

Optimised for embedding and semantic retrieval. Written by `populate_facts.py` as a **full overwrite** — every run regenerates the complete table from all current `facility_records`.

| Column | Type | Description |
|--------|------|-------------|
| `fact_id` | String (PK) | UUID for this fact row |
| `facility_id` | String (FK) | → `facility_records.facility_id` |
| `fact_text` | String | Natural language sentence to be embedded |
| `fact_type` | String | `"summary"`, `"description"`, `"procedure"`, `"equipment"`, `"capability"`, or `"specialty"` |

**Maximum 6 rows per facility.** The `fact_type` metadata column enables precise filtered Vector Search: an agent querying for `"MRI equipment"` can filter `fact_type = "equipment"` to retrieve only equipment facts, eliminating false-positive matches from summary or description text.

### 3. `regional_insights` — Text-to-SQL / Genie Analytics Table

Pre-aggregated analytics designed for Databricks Genie (Text-to-SQL). Contains **only 3 insight categories** (`overview`, `operator`, `specialty`).

| Column | Type | Description |
|--------|------|-------------|
| `country` | String | Grouping dimension — always `"Ghana"` |
| `state` | String | Grouping dimension — Ghana region |
| `city` | String | Grouping dimension — null means state-level total |
| `insight_category` | String | `"overview"`, `"operator"`, or `"specialty"` |
| `insight_value` | String | `"all_facilities"` / `"public"` / `"private"` / specialty name |
| `facility_count` | Integer | Distinct facilities in this slice |
| `total_capacity` | Integer | SUM of bed capacity (null for specialty rows) |
| `total_doctors` | Integer | SUM of doctor counts (null for specialty rows) |

**Example Genie SQL queries:**

```sql
-- "How many hospitals are in Accra?"
SELECT facility_count FROM regional_insights
WHERE city = 'Accra' AND insight_category = 'overview';

-- "Compare public vs private facilities in Ashanti"
SELECT insight_value, facility_count, total_capacity, total_doctors
FROM regional_insights
WHERE state = 'Ashanti' AND insight_category = 'operator';

-- "Which regions have the most ophthalmology facilities?"
SELECT state, facility_count FROM regional_insights
WHERE insight_category = 'specialty' AND insight_value = 'ophthalmology'
ORDER BY facility_count DESC;
```

---

## Dual Retrieval Architecture

| Query Type | Retrieval Method | Table Used | Example Query |
|------------|-----------------|------------|---------------|
| **Semantic / Qualitative** | Vector Search (RAG) | `facility_facts` | *"Find clinics near Accra that do cardiac surgery"* |
| **Narrative / Mission** | Vector Search (RAG) | `facility_facts` (fact_type=`description`) | *"Find facilities focused on maternal and child health"* |
| **Quantitative / Statistical** | Text-to-SQL (Genie) | `regional_insights` | *"How many public hospitals are in Ashanti?"* |
| **Facility Lookup** | Direct SQL or RAG | `facility_records` / `facility_facts` | *"What services does Korle Bu Teaching Hospital offer?"* |
| **Anomaly Detection** | Agent-side LLM reasoning | Both tables | *"Which clinics claim surgeries but have no beds?"* |

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
LLM_ENDPOINT=databricks-meta-llama-3-1-70b-instruct
MAX_WORKERS=6                               # Parallel extraction threads
MAX_PROCESS_ROWS=797                        # Max unique rows to process per run (max = 797)
GEMINI_API_KEY=AIzaXXXXXXXXXXXXXXXXXXXXX   # For Gemini location resolver (15 RPM free tier)
LOCATION_IQ_ACCESS_TOKEN=pk.XXXXXXXX       # For LocationIQ geocoding (2 req/s free tier)
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

# 4. Run Stage 1–4: Extract & save facility_records
uv run facility_record_generator.py

# 5. Run Stage 5: Generate facility_facts from all records
uv run populate_facts.py

# 6. Run Stage 6: Compute regional_insights aggregations
uv run compute_regional_insights.py
```

Each script is **idempotent via full overwrite** — re-running any script will completely regenerate its target table from scratch. There is no checkpointing. To process more rows, increase `MAX_PROCESS_ROWS` in `.env` and re-run `facility_record_generator.py`.

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **`summary` and `description` are separate fact_types** | `summary` is optimised for identity/type/affiliation matching. `description` is optimised for narrative/mission tone matching. Separating them prevents cross-contamination in top-k retrieval and allows the agent to filter by intent. |
| **No "Unknown" or "null" in `fact_text`** | If data is missing, the row is silently skipped. This prevents the Vector DB from returning false-positive matches on the word "unknown". |
| **Geographic context in every `fact_text`** | City, state, and country are embedded directly in every sentence. This enables geographic similarity matching without structured filters. |
| **5-step location cascade (dict → Gemini → LocationIQ)** | Deterministic lookup is free and instant; Gemini only fires for edge cases. LocationIQ is called last — only for lat/lon — so its quota is protected. This gives maximum coverage with minimum cost. |
| **`StrictThrottle` over Token Bucket for Gemini** | A Token Bucket allows bursty firing (all 6 workers immediately consume tokens). Google's quota is a strict 60-second rolling window that punishes bursts. `StrictThrottle` enforces one call every 4.28s across all workers, which is mathematically guaranteed to stay under 15 RPM regardless of concurrency. |
| **`_GARBAGE_KEYWORDS` as a shared constant** | Both `extractor.py` and `merger.py` import the same list. This guarantees consistent cleaning behaviour at both the extraction stage (first pass) and the merge stage (safety-net second pass). |
| **Deduplication preserves complementary data** | Multiple scraped rows for the same facility often have complementary non-null fields. Merging them column-by-column (first non-null wins per field) maximises information density per facility instead of arbitrarily picking one row. |
| **Single final overwrite at run end** | Mid-run batched overwrites with `mode="overwrite"` silently destroy earlier batches. Writing once at the end guarantees all processed rows appear in the final table. |
| **`regional_insights` uses Text-to-SQL, not RAG** | Vector Search is mathematically unreliable for counting, ranking, and aggregation. Text-to-SQL gives exact, provably correct quantitative answers. |
| **Overcounting prevention in `regional_insights`** | `total_capacity` and `total_doctors` are `NULL` for array-based dimensions (specialty) to prevent a single hospital's metrics from being multiplied across its specialties. |
| **`facility_id = CSV pk_unique_id`** | Eliminates the need for a separate `source_row_id`. The deduplicator guarantees exactly one merged row per `pk_unique_id`, so this ID is stable and collision-free. |
| **Three separate runner scripts** | `facility_record_generator.py`, `populate_facts.py`, and `compute_regional_insights.py` are fully independent. This allows any single stage to be re-run in isolation without triggering expensive LLM extraction again. |
