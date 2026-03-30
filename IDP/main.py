#!/usr/bin/env python3
"""
main.py — End-to-end IDP pipeline orchestration for Med-Atlas-AI.

Stages
------
1. Initialize Database → Register schemas for target Delta tables
2. Load CSV → Read structured raw data into memory
3. Checkpoint → Identify which CSV rows have already been processed
4. Extraction Chain → Process new rows through the 4-step LLM pipeline
5. Merge & Shape → Consolidate LLM outputs into `facility_records`
6. Fact Generation → Paraphrase and extract scalar facts into `facility_facts`
7. Regional Insights → Aggregate macro analytics, save to `regional_insights`, 
   and inject location-aware summary sentences into `facility_facts` for RAG.
"""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

from pyspark.sql import functions as F

from storage.database import DatabricksDatabase
from storage.models import (
    FACILITY_RECORDS_SCHEMA,
    FACILITY_FACTS_SCHEMA,
    REGIONAL_INSIGHTS_SCHEMA,
)
from pipeline.loader import load_csv_data
from pipeline.extractor import LLMExtractor
from pipeline.merger import merge_extraction_results
from pipeline.fact_generator import generate_facts

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("med-atlas-idp")


def main() -> None:
    """Run the full IDP pipeline."""
    t0 = time.time()

    # ── 1. Database setup ────────────────────────────────────────────
    logger.info("═══ Stage 1: Initialising database ═══")
    db = DatabricksDatabase()

    # Ensure target tables exist
    db.create_table_if_not_exists("facility_records", FACILITY_RECORDS_SCHEMA)
    db.create_table_if_not_exists("facility_facts", FACILITY_FACTS_SCHEMA)
    db.create_table_if_not_exists("regional_insights", REGIONAL_INSIGHTS_SCHEMA)

    # ── 2. Load CSV ──────────────────────────────────────────────────
    logger.info("═══ Stage 2: Loading CSV ═══")
    rows = load_csv_data()

    # ── 3 & 4. Extraction + Merge (parallel) ────────────────────────
    max_workers = int(os.getenv("MAX_WORKERS", "4"))
    logger.info("═══ Stage 3-4: Identify pending rows for extraction ═══")

    # Checkpointing: Filter out already processed records
    processed_ids = set()
    if db._table_exists("facility_records"):
        try:
            existing_df = db.read_delta("facility_records").select("facility_id")
            processed_ids = {r["facility_id"] for r in existing_df.collect()}
            logger.info("Checkpoint: Found %d already processed rows in facility_records", len(processed_ids))
        except Exception as e:
            logger.warning("Could not read existing facility_records for checkpointing: %s", e)

    pending_rows = []
    for r in rows:
        row_id = str(r.get("unique_id") or r.get("pk_unique_id") or "")
        if row_id not in processed_ids:
            pending_rows.append(r)
            
    rows = pending_rows

    max_process_rows = os.getenv("MAX_PROCESS_ROWS")
    max_process_rows = min(int(max_process_rows), 987)
    print("max_process_rows = ", max_process_rows)
    limit = None
    if max_process_rows:
        try:
            limit = int(max_process_rows)
            if len(processed_ids) >= limit:
                logger.info(
                    "\u2550\u2550\u2550 MAX_PROCESS_ROWS=%d reached (%d rows already in facility_records). "
                    "Nothing to do. \u2550\u2550\u2550",
                    limit, len(processed_ids),
                )
                _print_summary(db)
                return
            # Cap pending rows to the remaining budget
            remaining = limit - len(processed_ids)
            rows = rows[:remaining]
            logger.info(
                "MAX_PROCESS_ROWS=%d, already processed=%d, will process %d more rows.",
                limit, len(processed_ids), len(rows),
            )
        except ValueError:
            logger.warning("MAX_PROCESS_ROWS is not a valid integer. Processing all pending rows.")

    total_rows = len(rows)
    logger.info("Pending rows to process: %d", total_rows)

    if total_rows == 0:
        logger.info("═══ All rows already processed. Nothing to do. ═══")
        _print_summary(db)
        return

    # Flag: track whether any new rows were actually written this run
    new_rows_written = False

    BATCH_SIZE = 50
    facility_records_batch: List[Dict[str, Any]] = []
    facts_batch: List[Dict[str, Any]] = []
    done = 0

    if total_rows > 0:
        logger.info("═══ Starting Extraction (Batched Saves) ═══")
        
        logger.info("Connecting to Databricks Model Serving endpoint...")
        extractor = LLMExtractor()
        
        def _process_row(
            args: Tuple[int, Dict[str, Any]]
        ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
            """Process a single row: extract → merge → generate facts."""
            idx, row = args
            row_id = row.get("unique_id") or row.get("pk_unique_id") or str(idx)
            try:
                extraction = extractor.process_row(row)
                record = merge_extraction_results(extraction, row)
                facts = generate_facts(record)
                return record, facts
            except Exception as exc:
                logger.error(
                    "Row %d (id=%s, name=%s) failed: %s — skipping",
                    idx + 1, row_id, row.get("name", "?"), exc,
                )
                return None, []

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_process_row, (i, r)): i
                for i, r in enumerate(rows)
            }
            for future in as_completed(futures):
                record, facts = future.result()
                done += 1
                if record:
                    facility_records_batch.append(record)
                    facts_batch.extend(facts)
                
                if done % 10 == 0 or done == total_rows:
                    logger.info("Progress: %d/%d pending rows done", done, total_rows)
                
                # Batch save
                if len(facility_records_batch) >= BATCH_SIZE or done == total_rows:
                    if facility_records_batch:
                        records_df = db.spark.createDataFrame(facility_records_batch, FACILITY_RECORDS_SCHEMA)
                        db.append_delta(records_df, "facility_records")
                        facility_records_batch.clear()
                        new_rows_written = True  # New data landed — Stage 7 will run
                    if facts_batch:
                        facts_df = db.spark.createDataFrame(facts_batch, FACILITY_FACTS_SCHEMA)
                        db.append_delta(facts_df, "facility_facts")
                        facts_batch.clear()

        logger.info("Extraction and batch-saving complete.")

    # ── 7. Regional insights ───────────────────────────────────────────
    if new_rows_written:
        logger.info("═══ Stage 7: Aggregating regional insights (✔ new data written) ═══")
        _compute_regional_insights(db)
    else:
        logger.info("═══ Stage 7: Skipped — no new rows were written this run. ═══")


    elapsed = time.time() - t0
    logger.info("═══ Pipeline complete in %.1f seconds ═══", elapsed)
    _print_summary(db)


# ── Regional insights aggregation ────────────────────────────────────────

def _compute_regional_insights(db) -> None:
    """Aggregate facility_records into multi-dimensional regional_insights."""
    try:
        records_df = db.read_delta("facility_records")
    except Exception:
        logger.warning("facility_records not found — skipping regional insights")
        return

    # Base select needed for all aggregations
    base_df = records_df.select(
        "facility_id", "country", "state", "city", 
        "no_beds", "number_doctors", "operator_type",
        "specialties", "procedures", "equipment", "capabilities"
    )
    
    insights_dfs = []

    # 1. OVERVIEW (Totals per region)
    overview_df = base_df.groupBy("country", "state", "city").agg(
        F.countDistinct("facility_id").alias("facility_count"),
        F.sum("no_beds").alias("total_beds"),
        F.sum("number_doctors").alias("total_doctors"),
        F.collect_set("facility_id").alias("contributing_facility_ids")
    )
    overview_df = overview_df.select(
        "country", "state", "city",
        F.lit("overview").alias("insight_category"),
        F.lit("all_facilities").alias("insight_value"),
        "facility_count", "total_beds", "total_doctors", "contributing_facility_ids"
    )
    insights_dfs.append(overview_df)

    # 2. OPERATOR TYPE
    operator_df = base_df.filter(F.col("operator_type").isNotNull()).groupBy("country", "state", "city", "operator_type").agg(
        F.countDistinct("facility_id").alias("facility_count"),
        F.sum("no_beds").alias("total_beds"),
        F.sum("number_doctors").alias("total_doctors"),
        F.collect_set("facility_id").alias("contributing_facility_ids")
    )
    operator_df = operator_df.select(
        "country", "state", "city",
        F.lit("operator").alias("insight_category"),
        F.col("operator_type").alias("insight_value"),
        "facility_count", "total_beds", "total_doctors", "contributing_facility_ids"
    )
    insights_dfs.append(operator_df)

    # Helper function for array explosions (total_beds and total_doctors are set to NULL to prevent statistical overcounting)
    def _explode_and_agg(column_name: str, category_name: str):
        exploded = base_df.withColumn("item", F.explode_outer(F.col(column_name))).filter(F.col("item").isNotNull())
        grouped = exploded.groupBy("country", "state", "city", "item").agg(
            F.countDistinct("facility_id").alias("facility_count"),
            F.collect_set("facility_id").alias("contributing_facility_ids")
        )
        from pyspark.sql.types import IntegerType
        return grouped.select(
            "country", "state", "city",
            F.lit(category_name).alias("insight_category"),
            F.col("item").alias("insight_value"),
            "facility_count",
            F.lit(None).cast(IntegerType()).alias("total_beds"),
            F.lit(None).cast(IntegerType()).alias("total_doctors"),
            "contributing_facility_ids"
        )

    # 3. SPECIALTIES (camelCase enum values — groups correctly)
    insights_dfs.append(_explode_and_agg("specialties", "specialty"))
    # NOTE: procedure, equipment, capability categories removed —
    # free-text strings create noisy duplicates (e.g. "Offers internal
    # medicine services" vs "Provides internal medicine services") and
    # bury numeric data in prose.  Genie handles those queries via
    # facility_records + facility_facts directly.

    # Union all slices together
    final_insights = insights_dfs[0]
    for df in insights_dfs[1:]:
        final_insights = final_insights.unionByName(df)

    # Save exactly to the BI schema order
    ordered = final_insights.select(
        "country", "state", "city", 
        "insight_category", "insight_value", 
        "facility_count", "total_beds", "total_doctors", 
        "contributing_facility_ids"
    )

    db.write_delta(ordered, "regional_insights")
    row_count = ordered.count()
    logger.info("regional_insights (BI Table): %d multi-dimensional rows generated.", row_count)


# ── Summary ──────────────────────────────────────────────────────────────

def _print_summary(db) -> None:
    """Print row counts for all output tables."""
    tables = [
        "facility_records",
        "facility_facts",
        "regional_insights",
    ]
    logger.info("─── Final table row counts ───")
    for t in tables:
        if db._table_exists(t):
            try:
                count = db.read_delta(t).count()
                logger.info("  %-30s %d rows", t, count)
            except Exception:
                logger.info("  %-30s (error reading)", t)
        else:
            logger.info("  %-30s (not found)", t)


if __name__ == "__main__":
    main()
