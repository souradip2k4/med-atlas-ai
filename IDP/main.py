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
    done = 0

    if total_rows > 0:
        logger.info("═══ Starting Extraction (Batched Saves) ═══")
        
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
                    logger.info("Progress: %d/%d pending rows done", done, total_rows)
                
                # Batch save
                if len(facility_records_batch) >= BATCH_SIZE or done == total_rows:
                    if facility_records_batch:
                        records_df = db.spark.createDataFrame(facility_records_batch, FACILITY_RECORDS_SCHEMA)
                        db.append_delta(records_df, "facility_records")
                        facility_records_batch.clear()
                        new_rows_written = True

        logger.info("Extraction and batch-saving complete.")

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
