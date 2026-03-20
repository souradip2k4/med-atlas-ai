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
    max_workers = int(os.getenv("MAX_WORKERS", "8"))
    logger.info(
        "═══ Stage 3-4: LLM extraction + merge (parallel, workers=%d) ═══",
        max_workers,
    )
    extractor = LLMExtractor()

    # Collect rows as dicts
    rows: List[Dict[str, Any]] = [
        row.asDict() for row in raw_df.collect()
    ]

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

    facility_records: List[Dict[str, Any]] = []
    all_facts: List[Dict[str, Any]] = []
    done = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_process_row, (i, r)): i
            for i, r in enumerate(rows)
        }
        for future in as_completed(futures):
            record, facts = future.result()
            done += 1
            if record:
                facility_records.append(record)
                all_facts.extend(facts)
            if done % 50 == 0 or done == total_rows:
                logger.info(
                    "Progress: %d/%d rows done (%d records, %d facts so far)",
                    done, total_rows, len(facility_records), len(all_facts),
                )

    logger.info(
        "Extraction complete: %d records, %d facts",
        len(facility_records), len(all_facts),
    )

    # ── Write facility_records ───────────────────────────────────────
    if facility_records:
        logger.info("═══ Writing facility_records ═══")
        records_df = db.spark.createDataFrame(facility_records, FACILITY_RECORDS_SCHEMA)
        db.write_delta(records_df, "facility_records")

    # ── Write facility_facts ─────────────────────────────────────────
    if all_facts:
        logger.info("═══ Writing facility_facts ═══")
        facts_df = db.spark.createDataFrame(all_facts, FACILITY_FACTS_SCHEMA)
        db.write_delta(facts_df, "facility_facts")

    # ── 6. Generate embeddings ───────────────────────────────────────
    logger.info("═══ Stage 6: Generating embeddings ═══")
    if all_facts:
        emb_gen = EmbeddingGenerator()
        embedding_records = emb_gen.generate_embeddings(all_facts)

        if embedding_records:
            logger.info("Writing %d embedding records", len(embedding_records))
            emb_df = db.spark.createDataFrame(
                embedding_records, FACILITY_EMBEDDINGS_SCHEMA
            )
            db.write_delta(emb_df, "facility_embeddings")
    else:
        logger.warning("No facts to embed — skipping embedding stage")

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
