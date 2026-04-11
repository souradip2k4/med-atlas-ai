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
    BooleanType,
    ArrayType,
    MapType,
    TimestampType,
    DoubleType,
)

# ---------------------------------------------------------------------------
# facility_records — structured extraction output
# ---------------------------------------------------------------------------
FACILITY_RECORDS_SCHEMA = StructType([
    # ── Identity ──
    StructField("facility_id", StringType(), nullable=False),

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

    # ── Geospatial ──
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),

    # ── Contact ──
    StructField("phone_numbers", ArrayType(StringType()), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("websites", ArrayType(StringType()), nullable=True),
    StructField("social_links", MapType(StringType(), StringType()), nullable=True),
    StructField("officialWebsite", StringType(), nullable=True),

    # ── Meta ──
    StructField("year_established", IntegerType(), nullable=True),
    StructField("accepts_volunteers", BooleanType(), nullable=True),
    StructField("capacity", IntegerType(), nullable=True),
    StructField("no_doctors", IntegerType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("mission_statement", StringType(), nullable=True),
    StructField("affiliation_types", ArrayType(StringType()), nullable=True),
    StructField("operator_type", StringType(), nullable=True),
    StructField("facility_type", StringType(), nullable=True),


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
    StructField("source_text", StringType(), nullable=True),
])

# ---------------------------------------------------------------------------
# regional_insights — aggregated regional analytics
# ---------------------------------------------------------------------------
REGIONAL_INSIGHTS_SCHEMA = StructType([
    StructField("country", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("insight_category", StringType(), nullable=False),
    StructField("insight_value", StringType(), nullable=False),
    StructField("facility_count", IntegerType(), nullable=True),
    StructField("total_capacity", IntegerType(), nullable=True),
    StructField("total_doctors", IntegerType(), nullable=True),
    StructField("contributing_facility_ids", ArrayType(StringType()), nullable=True),
])