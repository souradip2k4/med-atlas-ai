#!/usr/bin/env python3
"""
populate_facts.py — Standalone script to populate the facility_facts table from facility_records.
"""

import logging
import os
import time
from typing import Any, Dict, List

from dotenv import load_dotenv

from storage.database import DatabricksDatabase
from storage.models import (
    FACILITY_FACTS_SCHEMA,
)
from pipeline.fact_generator import generate_facts

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("med-atlas-idp-facts")

def main() -> None:
    t0 = time.time()
    
    logger.info("═══ Stage 1: Initialising database ═══")
    db = DatabricksDatabase()
    
    db.create_table_if_not_exists("facility_facts", FACILITY_FACTS_SCHEMA)
    
    if not db._table_exists("facility_records"):
        logger.error("facility_records table does not exist. Please run facility_record_generator.py.py first.")
        return
        
    logger.info("═══ Stage 2: Reading existing data ═══")
    records_df = db.read_delta("facility_records")
    records = [row.asDict(recursive=True) for row in records_df.collect()]
    
    processed_ids = set()
    if db._table_exists("facility_facts"):
        try:
            existing_facts_df = db.read_delta("facility_facts").select("facility_id").distinct()
            processed_ids = {r["facility_id"] for r in existing_facts_df.collect()}
            logger.info("Checkpoint: Found %d unique facilities already processed in facility_facts", len(processed_ids))
        except Exception as e:
            logger.warning("Could not read existing facility_facts for checkpointing: %s", e)
            
    pending_records = []
    for r in records:
        if r["facility_id"] not in processed_ids:
            pending_records.append(r)
            


    total_rows = len(pending_records)
    logger.info("Pending facility records to process for facts: %d", total_rows)

    if total_rows == 0:
        logger.info("═══ All facilities already processed. Nothing to do. ═══")
        _print_summary(db)
        return
        
    logger.info("═══ Starting Fact Generation ═══")
    BATCH_SIZE = 50
    facts_batch: List[Dict[str, Any]] = []
    done = 0
    
    for record in pending_records:
        try:
            # Keep logic preserved as requested
            facts = generate_facts(record)
            facts_batch.extend(facts)
        except Exception as exc:
            logger.error(
                "Facility %s (name=%s) failed fact generation: %s — skipping",
                record.get("facility_id", "?"), record.get("facility_name", "?"), exc,
            )
            
        done += 1
        if done % 10 == 0 or done == total_rows:
            logger.info("Progress: %d/%d pending facilities done", done, total_rows)
            
        if (done % BATCH_SIZE == 0 or done == total_rows) and facts_batch:
            facts_df = db.spark.createDataFrame(facts_batch, FACILITY_FACTS_SCHEMA)
            db.append_delta(facts_df, "facility_facts")
            facts_batch.clear()

    logger.info("Fact generation and batch-saving complete.")
    elapsed = time.time() - t0
    logger.info("═══ Pipeline complete in %.1f seconds ═══", elapsed)
    _print_summary(db)

def _print_summary(db) -> None:
    """Print row counts for facts table."""
    logger.info("─── Final table row counts ───")
    if db._table_exists("facility_facts"):
        try:
            count = db.read_delta("facility_facts").count()
            distinct_facilities = db.read_delta("facility_facts").select("facility_id").distinct().count()
            logger.info("  %-30s %d total facts (%d facilities)", "facility_facts", count, distinct_facilities)
        except Exception:
            logger.info("  %-30s (error reading)", "facility_facts")
    else:
        logger.info("  %-30s (not found)", "facility_facts")

if __name__ == "__main__":
    main()
