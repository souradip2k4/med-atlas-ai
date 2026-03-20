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

    logger.info("Loaded %d rows × %d columns", len(pdf), len(pdf.columns))

    # ── Convert to Spark ─────────────────────────────────────────────
    sdf = db.spark.createDataFrame(pdf.astype(object).where(pdf.notna(), None))

    # ── Write as Delta table ─────────────────────────────────────────
    db.write_delta(sdf, "raw_facilities")

    return sdf
