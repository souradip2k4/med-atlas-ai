"""
loader.py — Load CSV data and write to Delta table `raw_facilities`.
"""

import os
import re
import logging

import pandas as pd
from pyspark.sql import DataFrame

from storage.database import DatabricksDatabase

logger = logging.getLogger(__name__)


def load_csv_to_delta(db: DatabricksDatabase, csv_path: str | None = None) -> DataFrame:
    """
    Load the facility CSV file, clean NULLs, and persist as
    the ``raw_facilities`` Delta table.

    Parameters
    ----------
    db : DatabricksDatabase
        Active database manager.
    csv_path : str, optional
        Path to the CSV file.  Falls back to the ``CSV_PATH`` env var.

    Returns
    -------
    pyspark.sql.DataFrame
        The loaded Spark DataFrame.
    """
    if csv_path is None:
        csv_path = os.getenv("CSV_PATH", "Virtue Foundation Ghana v0.3 - Sheet1.csv")

    logger.info("Loading CSV from %s", csv_path)

    # ── Read with Pandas (better NULL handling for messy CSVs) ────────
    pdf = pd.read_csv(csv_path, dtype=str, keep_default_na=True)

    # Normalise column names: strip, replace spaces + special chars with underscore
    pdf.columns = [
        re.sub(r'[^\w]', '_', c.strip()).strip('_').lower()
        for c in pdf.columns
    ]

    # Replace the various NULL representations with None
    null_tokens = {"null", "NULL", "None", "none", "N/A", "n/a", ""}
    for col in pdf.columns:
        pdf[col] = pdf[col].apply(
            lambda v: None if pd.isna(v) or (isinstance(v, str) and v.strip() in null_tokens) else v
        )

    csv_row_count = len(pdf)
    logger.info("Loaded %d rows × %d columns", csv_row_count, len(pdf.columns))

    # ── Skip write if table already fully loaded ──────────────────────
    if db._table_exists("raw_facilities"):
        existing_count = db.read_delta("raw_facilities").count()
        if existing_count == csv_row_count:
            logger.info(
                "raw_facilities already contains %d rows (matches CSV). Skipping write.",
                existing_count,
            )
            return db.read_delta("raw_facilities")

    # ── Convert to Spark ─────────────────────────────────────────────
    sdf = db.spark.createDataFrame(pdf.astype(object).where(pdf.notna(), None))

    # Drop columns that cause Databricks catalog issues or are not needed
    drop_cols = {"logo", "source_url", "mongo_db", "content_table_id"}
    existing_drop = [c for c in sdf.columns if c in drop_cols]
    if existing_drop:
        logger.info("Dropping non-essential columns from raw_facilities: %s", existing_drop)
        sdf = sdf.drop(*existing_drop)

    # ── Write as Delta table ─────────────────────────────────────────
    db.write_delta(sdf, "raw_facilities")

    return sdf
