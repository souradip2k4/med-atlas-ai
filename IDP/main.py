#!/usr/bin/env python3
"""
main.py — End-to-end IDP pipeline orchestration.

Stages
------
1. Load CSV → ``raw_facilities`` Delta table
2. Read rows, synthesize text (with normalization)
3. Process each row through the 4-step LLM extraction chain
4. Merge → ``facility_records``
5. Generate atomic facts (with paraphrasing) → ``facility_facts``
6. Generate embeddings (batched) → ``facility_embeddings``
7. Aggregate regional insights → ``regional_insights``
8. Create Vector Search index (precomputed embeddings)
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
            existing_df = db.read_delta("facility_records").select("source_row_id")
            processed_ids = {r["source_row_id"] for r in existing_df.collect()}
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
    """Aggregate facility_records into regional_insights."""
    try:
        records_df = db.read_delta("facility_records")
    except Exception:
        logger.warning("facility_records not found — skipping regional insights")
        return

    # Explode specialties and group by country/state/city/specialty
    exploded = (
        records_df
        .select("facility_id", "source_row_id", "country", "state", "city", "specialties")
        .withColumn("specialty", F.explode_outer(F.col("specialties")))
    )

    insights = (
        exploded
        .groupBy("country", "state", "city", "specialty")
        .agg(
            F.countDistinct("facility_id").alias("facility_count"),
            F.collect_set("facility_id").alias("contributing_facility_ids"),
            F.collect_set("source_row_id").alias("contributing_source_row_ids")
        )
        .withColumn("coverage_score", F.lit(None).cast("float"))
        .withColumn("gap_flag", F.when(F.col("facility_count") < 2, True).otherwise(False))
        .withColumn(
            "risk_level",
            F.when(F.col("facility_count") == 1, "high")
            .when(F.col("facility_count") < 3, "medium")
            .otherwise("low"),
        )
        .withColumn(
            "recommendation",
            F.when(F.col("facility_count") == 1, "Single provider — consider capacity expansion")
            .when(F.col("facility_count") < 3, "Limited coverage — monitor access")
            .otherwise("Coverage appears adequate"),
        )
    )

    import uuid
    from pyspark.sql.types import StringType

    # Select in schema order
    ordered = insights.select(
        "country", "state", "city", "specialty",
        "facility_count", "coverage_score",
        "gap_flag", "risk_level", "recommendation",
        "contributing_facility_ids", "contributing_source_row_ids"
    )

    # 1. Save numerical table for BI dashboards
    db.write_delta(ordered, "regional_insights")
    row_count = ordered.count()
    logger.info("regional_insights: %d rows", row_count)

    # 2. Define a UDF to generate UUIDs for the facts
    uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())

    # 3. Shape the insights into the facility_facts schema for Vector Search
    insights_facts = ordered.select(
        uuid_udf().alias("fact_id"),
        F.lit("REGIONAL_AGGREGATE").alias("facility_id"),
        
        # Synthesize human-readable English sentences
        F.concat(
            F.lit("In "), 
            F.coalesce(F.col("state"), F.lit("Unknown Region")), F.lit(", "), 
            F.coalesce(F.col("city"), F.lit("Unknown City")), F.lit(" ("), 
            F.coalesce(F.col("country"), F.lit("Ghana")), F.lit("), "),
            F.lit("there are "), F.col("facility_count").cast(StringType()), 
            F.lit(" facilities offering "), F.coalesce(F.col("specialty"), F.lit("general services")), 
            F.lit(". Regional Risk Level: "), F.coalesce(F.col("risk_level"), F.lit("unknown")),
            F.lit(". Strategic Recommendation: "), F.coalesce(F.col("recommendation"), F.lit("None"))
        ).alias("fact_text"),
        
        F.lit("regional_insight").alias("fact_type"),
        
        # Collapse the array into a comma-separated string of CSV row IDs (e.g. "15, 30, 42")
        F.concat_ws(",", F.col("contributing_source_row_ids")).alias("source_row_id"),
        
        F.lit("regional_aggregation").alias("source_column"),
        
        # Clean mathematical statement instead of hacking UUIDs here
        F.lit("Aggregated mathematically").alias("source_text")
    )

    # 4. Delete old regional insights to prevent accidental duplication
    if db._table_exists("facility_facts"):
        try:
            db.execute_sql(f"DELETE FROM {db.fqn('facility_facts')} WHERE fact_type = 'regional_insight'")
            logger.info("Deleted old regional_insights from facility_facts.")
        except Exception as e:
            logger.warning("Could not delete old regional_insights from facility_facts: %s", e)

    # 5. Append directly to the facts table!
    db.append_delta(insights_facts, "facility_facts")
    logger.info("Injected %d regional insights directly into facility_facts for Vector Search.", row_count)


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
