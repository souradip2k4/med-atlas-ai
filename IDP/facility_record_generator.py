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
)
from pipeline.loader import load_csv_data
from pipeline.deduplicator import deduplicate_rows
from pipeline.extractor import LLMExtractor
from pipeline.merger import merge_extraction_results

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

    db.create_table_if_not_exists("facility_records", FACILITY_RECORDS_SCHEMA)

    # ── 2. Load CSV ──────────────────────────────────────────────────
    logger.info("═══ Stage 2: Loading CSV ═══")
    rows = load_csv_data()

    # ── 2b. Deduplication ────────────────────────────────────────────
    logger.info("═══ Stage 2b: Deduplicating rows by pk_unique_id ═══")
    rows = deduplicate_rows(rows)
    logger.info("Processing %d unique facilities.", len(rows))

    # ── 3 & 4. Extraction + Merge (parallel) ────────────────────────
    max_workers = int(os.getenv("MAX_WORKERS", "4"))
    
    max_process_rows = os.getenv("MAX_PROCESS_ROWS")
    if max_process_rows:
        try:
            limit = int(max_process_rows)
            rows = rows[:limit]
            logger.info("MAX_PROCESS_ROWS=%d. Will process up to %d unique facilities in this run.", limit, limit)
        except ValueError:
            logger.warning("MAX_PROCESS_ROWS is not a valid integer. Processing all rows.")

    total_rows = len(rows)
    logger.info("Total rows to process: %d", total_rows)

    if total_rows == 0:
        logger.info("═══ No rows to process. ═══")
        _print_summary(db)
        return

    facility_records_batch: List[Dict[str, Any]] = []
    done = 0

    logger.info("═══ Starting Extraction ═══")
    logger.info("Connecting to Databricks Model Serving endpoint...")

    extractor = LLMExtractor()

    def _process_row(
        args: Tuple[int, Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """Process a single row: extract → merge."""
        idx, row = args
        row_id = row.get("unique_id") or row.get("pk_unique_id") or str(idx)
        try:
            extraction = extractor.process_row(row)
            record = merge_extraction_results(extraction, row)
            return record
        except Exception as exc:
            logger.error(
                "Row %d (id=%s, name=%s) failed: %s — skipping",
                idx + 1, row_id, row.get("name", "?"), exc,
            )
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_process_row, (i, r)): i
            for i, r in enumerate(rows)
        }
        for future in as_completed(futures):
            record = future.result()
            done += 1
            if record:
                facility_records_batch.append(record)
            if done % 10 == 0 or done == total_rows:
                logger.info("Progress: %d/%d rows done", done, total_rows)

    # Single overwrite at the very end — all rows in one shot
    if facility_records_batch:
        records_df = db.spark.createDataFrame(facility_records_batch, FACILITY_RECORDS_SCHEMA)
        db.write_delta(records_df, "facility_records", mode="overwrite")

    logger.info("Extraction complete. %d records written.", len(facility_records_batch))

    elapsed = time.time() - t0
    logger.info("═══ Pipeline complete in %.1f seconds ═══", elapsed)
    _print_summary(db)


# ── Summary ──────────────────────────────────────────────────────────────

def _print_summary(db) -> None:
    """Print row counts for all output tables."""
    tables = [
        "facility_records",
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
