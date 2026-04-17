#!/usr/bin/env python3
"""
compute_regional_insights.py — Standalone script to populate the regional_insights table.

Run this independently AFTER main.py has populated facility_records.
It aggregates facility_records into multi-dimensional regional analytics
and writes the results to the regional_insights Delta table.

Usage
-----
    uv run python IDP/compute_regional_insights.py

Stages
------
1. Ensure regional_insights table exists (creates if missing).
2. Read all facility_records.
3. Compute multi-dimensional aggregations:
     - Overview  (total facilities/capacity/doctors per region+city)
     - Operator  (breakdown by operator_type)
     - Specialty (one row per specialty per region+city)
4. Write results to regional_insights (full overwrite).
"""

import logging
import time

from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from storage.database import DatabricksDatabase
from storage.models import FACILITY_RECORDS_SCHEMA, REGIONAL_INSIGHTS_SCHEMA

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("med-atlas-regional-insights")


def compute_regional_insights(db: DatabricksDatabase) -> None:
    """Aggregate facility_records into multi-dimensional regional_insights."""

    logger.info("Reading facility_records...")
    try:
        records_df = db.read_delta("facility_records")
    except Exception as e:
        logger.error("Could not read facility_records: %s", e)
        raise

    row_count = records_df.count()
    logger.info("facility_records: %d rows loaded.", row_count)

    # Base select — only the columns needed for aggregations
    base_df = records_df.select(
        "facility_id", "country", "state", "city",
        "capacity", "no_doctors", "operator_type",
        "specialties", "procedures", "equipment", "capabilities",
    )

    insights_dfs = []

    # ── 1. OVERVIEW ─────────────────────────────────────────────────────────
    logger.info("Computing overview aggregation...")
    overview_df = base_df.groupBy("country", "state", "city").agg(
        F.countDistinct("facility_id").alias("facility_count"),
        F.sum("capacity").alias("total_capacity"),
        F.sum("no_doctors").alias("total_doctors"),
    )
    overview_df = overview_df.select(
        "country", "state", "city",
        F.lit("overview").alias("insight_category"),
        F.lit("all_facilities").alias("insight_value"),
        "facility_count", "total_capacity", "total_doctors",
    )
    insights_dfs.append(overview_df)

    # ── 2. OPERATOR TYPE ─────────────────────────────────────────────────────
    logger.info("Computing operator_type aggregation...")
    operator_df = (
        base_df
        .filter(F.col("operator_type").isNotNull())
        .groupBy("country", "state", "city", "operator_type")
        .agg(
            F.countDistinct("facility_id").alias("facility_count"),
            F.sum("capacity").alias("total_capacity"),
            F.sum("no_doctors").alias("total_doctors"),
        )
    )
    operator_df = operator_df.select(
        "country", "state", "city",
        F.lit("operator").alias("insight_category"),
        F.col("operator_type").alias("insight_value"),
        "facility_count", "total_capacity", "total_doctors",
    )
    insights_dfs.append(operator_df)

    # ── Helper: explode array column and aggregate ───────────────────────────
    def _explode_and_agg(column_name: str, category_name: str):
        """Explode an array column and count facilities per distinct value."""
        exploded = (
            base_df
            .withColumn("item", F.explode_outer(F.col(column_name)))
            .filter(F.col("item").isNotNull())
        )
        grouped = exploded.groupBy("country", "state", "city", "item").agg(
            F.countDistinct("facility_id").alias("facility_count"),
        )
        return grouped.select(
            "country", "state", "city",
            F.lit(category_name).alias("insight_category"),
            F.col("item").alias("insight_value"),
            "facility_count",
            F.lit(None).cast(IntegerType()).alias("total_capacity"),
            F.lit(None).cast(IntegerType()).alias("total_doctors"),
        )

    # ── 3. SPECIALTIES ───────────────────────────────────────────────────────
    # camelCase enum values — groups deterministically, unlike free-text strings.
    # NOTE: procedure, equipment, and capability columns are excluded because
    # those are free-text strings that generate noisy duplicates
    # (e.g., "Offers internal medicine services" ≠ "Provides internal medicine").
    # Genie Chat handles procedure/equipment queries directly via facility_records.
    logger.info("Computing specialty aggregation...")
    insights_dfs.append(_explode_and_agg("specialties", "specialty"))

    # ── Union all slices ─────────────────────────────────────────────────────
    logger.info("Unioning aggregation slices...")
    final_insights = insights_dfs[0]
    for df in insights_dfs[1:]:
        final_insights = final_insights.unionByName(df)

    # Enforce exact schema column order
    ordered = final_insights.select(
        "country", "state", "city",
        "insight_category", "insight_value",
        "facility_count", "total_capacity", "total_doctors",
    )

    logger.info("Writing regional_insights (full overwrite)...")
    db.write_delta(ordered, "regional_insights")
    written = ordered.count()
    logger.info("regional_insights: %d multi-dimensional rows written.", written)


def main() -> None:
    t0 = time.time()

    logger.info("═══ Regional Insights Aggregation Script ═══")

    db = DatabricksDatabase()

    # Ensure the target table exists (creates schema if first run)
    db.create_table_if_not_exists("regional_insights", REGIONAL_INSIGHTS_SCHEMA)

    # Verify source table is available
    if not db._table_exists("facility_records"):
        logger.error(
            "facility_records table does not exist. "
            "Run main.py first to populate facility data."
        )
        return

    compute_regional_insights(db)

    elapsed = time.time() - t0
    logger.info("═══ Done in %.1f seconds ═══", elapsed)

    # Print final row count
    try:
        count = db.read_delta("regional_insights").count()
        logger.info("regional_insights total rows: %d", count)
    except Exception as e:
        logger.warning("Could not read final count: %s", e)


if __name__ == "__main__":
    main()
