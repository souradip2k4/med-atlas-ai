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

from pipeline.facility_fact_generator import generate_facts

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
    
    pending_records = records

    total_rows = len(pending_records)
    logger.info("Total facility records to process for facts: %d", total_rows)
    
    if total_rows == 0:
        logger.info("═══ No facility records found. ═══")
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

    # Single overwrite at the very end — all rows in one shot
    if facts_batch:
        facts_df = db.spark.createDataFrame(facts_batch, FACILITY_FACTS_SCHEMA)
        db.write_delta(facts_df, "facility_facts", mode="overwrite")

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
