# Med-Atlas-AI: Intelligent Document Processing Pipeline

A Databricks-based IDP pipeline that processes healthcare facility data from Ghana, extracts structured information using LLMs, generates searchable facts with embeddings, and creates a vector search index for semantic retrieval.

## Architecture

```
CSV Data
  │
  ▼
┌─────────────────────────┐
│  1. loader.py           │  CSV → raw_facilities (Delta)
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  2. preprocessor.py     │  Row → normalised text block
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  3. extractor.py        │  4-step LLM extraction chain:
│                         │    ① Organization Extraction
│                         │    ② Facility Fact Extraction
│                         │    ③ Medical Specialty Extraction
│                         │    ④ Facility Structured Info
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  4. merger.py           │  Merge → facility_records (Delta)
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  5. fact_generator.py   │  Atomic facts (paraphrased) → facility_facts
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  6. embedding.py        │  Batched embeddings → facility_embeddings
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  7. vector_store.py     │  Vector Search index (precomputed)
└─────────────────────────┘
```

## Project Structure

```
Med-Atlas-AI/
├── config/
│   ├── __init__.py
│   ├── free_form.py                  # FacilityFacts model + prompt
│   ├── medical_specialties.py        # MedicalSpecialties model + prompt
│   ├── organization_extraction.py    # OrganizationExtractionOutput + prompt
│   └── facility_and_ngo_fields.py    # Facility / NGO models + prompt
├── pipeline/
│   ├── __init__.py
│   ├── loader.py                     # CSV → Delta table
│   ├── preprocessor.py               # Row text synthesis
│   ├── extractor.py                  # 4-step LLM extraction
│   ├── merger.py                     # Merge extraction outputs
│   ├── fact_generator.py             # Atomic fact generation
│   └── embedding.py                  # Embedding generation
├── storage/
│   ├── __init__.py
│   ├── database.py                   # Databricks session + Delta I/O
│   └── models.py                     # Table schemas
├── vector/
│   ├── __init__.py
│   └── vector_store.py               # Vector Search index
├── main.py                           # Pipeline orchestrator
├── requirements.txt
├── .env                              # Credentials (gitignored)
└── README.md
```

## Setup

```bash
# 1. Create virtual environment
python -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure .env
# Edit .env with your Databricks workspace host, token, catalog, schema
```

## Usage

```bash
# Run the full pipeline
python main.py
```

## Output Tables

| Table | Description |
|-------|-------------|
| `raw_facilities` | Raw CSV data as Delta |
| `facility_records` | Structured facility records with provenance |
| `facility_facts` | Atomic facts with paraphrased variants |
| `facility_embeddings` | Facts with precomputed embedding vectors |
| `regional_insights` | Aggregated regional analytics |

## Key Design Decisions

- **Manual embeddings** — Full control over embedding generation; no managed/auto embeddings
- **Multi-phrasing** — 2-3 paraphrased variants per fact for better retrieval recall
- **Provenance tracking** — `source_column`, `source_text` for UI citations
- **Per-field confidence** — Separate confidence scores for specialties, equipment, capabilities
- **Suspicious flagging** — Auto-detects rows with no extracted medical data
- **One row per LLM call** — No batching across rows to avoid cross-contamination
