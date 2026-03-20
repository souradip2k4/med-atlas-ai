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
    FACILITY_EMBEDDINGS_SCHEMA,
    REGIONAL_INSIGHTS_SCHEMA,
)
from pipeline.loader import load_csv_to_delta
from pipeline.extractor import LLMExtractor
from pipeline.merger import merge_extraction_results
from pipeline.fact_generator import generate_facts
from pipeline.embedding import EmbeddingGenerator
from vector.vector_store import VectorStore

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
    db.create_table_if_not_exists("facility_embeddings", FACILITY_EMBEDDINGS_SCHEMA)
    db.create_table_if_not_exists("regional_insights", REGIONAL_INSIGHTS_SCHEMA)

    # ── 2. Load CSV → raw_facilities ─────────────────────────────────
    logger.info("═══ Stage 2: Loading CSV → raw_facilities ═══")
    raw_df = load_csv_to_delta(db)
    total_rows = raw_df.count()
    logger.info("raw_facilities table: %d rows", total_rows)

    # ── 3 & 4. Extraction + Merge (parallel) ────────────────────────
    max_workers = int(os.getenv("MAX_WORKERS", "4"))
    logger.info(
        "═══ Stage 3-4: LLM extraction + merge (parallel, workers=%d) ═══",
        max_workers,
    )
    extractor = LLMExtractor()

    # Collect rows as dicts
    rows: List[Dict[str, Any]] = [
        row.asDict() for row in raw_df.collect()
    ]

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
    if max_process_rows:
        try:
            limit = int(max_process_rows)
            rows = rows[:limit]
            logger.info("MAX_PROCESS_ROWS=%d. Limiting pending processing to %d rows.", limit, len(rows))
        except ValueError:
            logger.warning("MAX_PROCESS_ROWS is not a valid integer. Processing all pending rows.")
            
    total_rows = len(rows)
    logger.info("Pending rows to process: %d", total_rows)

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

    BATCH_SIZE = 50
    facility_records_batch: List[Dict[str, Any]] = []
    facts_batch: List[Dict[str, Any]] = []
    done = 0

    if total_rows > 0:
        logger.info("═══ Starting Extraction (Batched Saves) ═══")
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
                    if facts_batch:
                        facts_df = db.spark.createDataFrame(facts_batch, FACILITY_FACTS_SCHEMA)
                        db.append_delta(facts_df, "facility_facts")
                        facts_batch.clear()

        logger.info("Extraction and batch-saving complete.")

    # ── 6. Generate embeddings ───────────────────────────────────────
    logger.info("═══ Stage 6: Generating embeddings ═══")
    if db._table_exists("facility_facts"):
        try:
            facts_df = db.read_delta("facility_facts")
            all_facts_dicts = [r.asDict() for r in facts_df.collect()]
            
            # Checkpoint for embeddings
            embedded_ids = set()
            if db._table_exists("facility_embeddings"):
                try:
                    emb_df_exist = db.read_delta("facility_embeddings").select("fact_id")
                    embedded_ids = {r["fact_id"] for r in emb_df_exist.collect()}
                    logger.info("Checkpoint: Found %d already embedded facts", len(embedded_ids))
                except Exception:
                    pass
            
            facts_to_embed = [f for f in all_facts_dicts if f["fact_id"] not in embedded_ids]
            
            if facts_to_embed:
                logger.info("Pending facts to embed: %d", len(facts_to_embed))
                emb_gen = EmbeddingGenerator()
                embedding_records = emb_gen.generate_embeddings(facts_to_embed)

                if embedding_records:
                    logger.info("Writing %d new embedding records", len(embedding_records))
                    emb_df = db.spark.createDataFrame(
                        embedding_records, FACILITY_EMBEDDINGS_SCHEMA
                    )
                    db.append_delta(emb_df, "facility_embeddings")
            else:
                logger.info("No new facts to embed.")
        except Exception as e:
            logger.warning("Could not process embeddings: %s", e)
    else:
        logger.warning("facility_facts table not found — skipping embedding stage")

    # ── 7. Regional insights ─────────────────────────────────────────
    logger.info("═══ Stage 7: Aggregating regional insights ═══")
    _compute_regional_insights(db)

    # ── 8. Vector Search index ───────────────────────────────────────
    logger.info("═══ Stage 8: Creating Vector Search index ═══")
    try:
        vs = VectorStore()
        vs.create_endpoint()
        vs.create_index()
        vs.sync_index()
        logger.info("Vector Search index ready")
    except Exception as e:
        logger.error("Vector Search setup failed: %s", e, exc_info=True)
        logger.info("You can create the index manually later via VectorStore()")

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
        .select("facility_id", "country", "state", "city", "specialties")
        .withColumn("specialty", F.explode_outer(F.col("specialties")))
    )

    insights = (
        exploded
        .groupBy("country", "state", "city", "specialty")
        .agg(
            F.countDistinct("facility_id").alias("facility_count"),
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
            .otherwise(None),
        )
    )

    # Select in schema order
    ordered = insights.select(
        "country", "state", "city", "specialty",
        "facility_count", "coverage_score",
        "gap_flag", "risk_level", "recommendation",
    )

    db.write_delta(ordered, "regional_insights")
    logger.info("regional_insights: %d rows", ordered.count())


# ── Summary ──────────────────────────────────────────────────────────────

def _print_summary(db) -> None:
    """Print row counts for all output tables."""
    tables = [
        "raw_facilities",
        "facility_records",
        "facility_facts",
        "facility_embeddings",
        "regional_insights",
    ]
    logger.info("─── Final table row counts ───")
    for t in tables:
        try:
            count = db.read_delta(t).count()
            logger.info("  %-30s %d rows", t, count)
        except Exception:
            logger.info("  %-30s (not found)", t)


if __name__ == "__main__":
    main()
