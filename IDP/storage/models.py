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
    StructField("contributing_facility_ids", ArrayType(StringType()), nullable=True),
    StructField("contributing_source_row_ids", ArrayType(StringType()), nullable=True),
])