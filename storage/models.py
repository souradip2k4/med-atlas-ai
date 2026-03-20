"""
Delta table schemas for the IDP pipeline.

Defines PySpark StructType schemas for:
  - raw_facilities
  - facility_records
  - facility_facts
  - facility_embeddings
  - regional_insights
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BooleanType,
    ArrayType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# raw_facilities — ingested CSV as-is
# ---------------------------------------------------------------------------
RAW_FACILITIES_SCHEMA = None  # inferred from CSV at load time

# ---------------------------------------------------------------------------
# facility_records — structured extraction output
# ---------------------------------------------------------------------------
FACILITY_RECORDS_SCHEMA = StructType([
    # ── Identity ──
    StructField("facility_id", StringType(), nullable=False),
    StructField("source_row_id", StringType(), nullable=False),

    # ── Core ──
    StructField("facility_name", StringType(), nullable=False),
    StructField("organization_type", StringType(), nullable=False),

    # ── Medical data ──
    StructField("specialties", ArrayType(StringType()), nullable=True),
    StructField("procedures", ArrayType(StringType()), nullable=True),
    StructField("equipment", ArrayType(StringType()), nullable=True),
    StructField("capabilities", ArrayType(StringType()), nullable=True),

    # ── Location ──
    StructField("address_line1", StringType(), nullable=True),
    StructField("address_line2", StringType(), nullable=True),
    StructField("address_line3", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("country_code", StringType(), nullable=True),

    # ── Contact ──
    StructField("phone_numbers", ArrayType(StringType()), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("websites", ArrayType(StringType()), nullable=True),
    StructField("officialWebsite", StringType(), nullable=True),

    # ── Meta ──
    StructField("year_established", IntegerType(), nullable=True),
    StructField("accepts_volunteers", BooleanType(), nullable=True),
    StructField("number_doctors", IntegerType(), nullable=True),
    StructField("capacity", IntegerType(), nullable=True),

    # ── Evidence & confidence ──
    StructField("evidence_text", StringType(), nullable=False),
    StructField("source_text", StringType(), nullable=True),
    StructField("source_column", StringType(), nullable=True),
    StructField("extraction_confidence", FloatType(), nullable=False),
    StructField("confidence_specialties", FloatType(), nullable=True),
    StructField("confidence_equipment", FloatType(), nullable=True),
    StructField("confidence_capabilities", FloatType(), nullable=True),

    # ── Suspicious flag ──
    StructField("is_suspicious", BooleanType(), nullable=True),
    StructField("suspicious_reason", StringType(), nullable=True),

    # ── Timestamps ──
    StructField("created_at", TimestampType(), nullable=True),
    StructField("updated_at", TimestampType(), nullable=True),
])

# ---------------------------------------------------------------------------
# facility_facts — atomic fact rows
# ---------------------------------------------------------------------------
FACILITY_FACTS_SCHEMA = StructType([
    StructField("fact_id", StringType(), nullable=False),
    StructField("facility_id", StringType(), nullable=False),
    StructField("fact_text", StringType(), nullable=False),
    StructField("fact_type", StringType(), nullable=False),
    StructField("source_row_id", StringType(), nullable=False),
    StructField("source_column", StringType(), nullable=True),
    StructField("source_text", StringType(), nullable=True),
    StructField("source_start", IntegerType(), nullable=True),
    StructField("source_end", IntegerType(), nullable=True),
])

# ---------------------------------------------------------------------------
# facility_embeddings — facts + precomputed embedding vectors
# ---------------------------------------------------------------------------
FACILITY_EMBEDDINGS_SCHEMA = StructType([
    StructField("fact_id", StringType(), nullable=False),
    StructField("facility_id", StringType(), nullable=False),
    StructField("fact_text", StringType(), nullable=False),
    StructField("fact_type", StringType(), nullable=False),
    StructField("embedding", ArrayType(FloatType()), nullable=False),
    StructField("source_row_id", StringType(), nullable=True),
])

# ---------------------------------------------------------------------------
# regional_insights — aggregated regional analytics
# ---------------------------------------------------------------------------
REGIONAL_INSIGHTS_SCHEMA = StructType([
    StructField("country", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("specialty", StringType(), nullable=True),
    StructField("facility_count", IntegerType(), nullable=True),
    StructField("coverage_score", FloatType(), nullable=True),
    StructField("gap_flag", BooleanType(), nullable=True),
    StructField("risk_level", StringType(), nullable=True),
    StructField("recommendation", StringType(), nullable=True),
])