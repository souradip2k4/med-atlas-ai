"""
loader.py — Load CSV data directly into memory.
"""

import os
import re
import logging
from typing import List, Dict, Any

import pandas as pd

logger = logging.getLogger(__name__)

def load_csv_data(csv_path: str | None = None) -> List[Dict[str, Any]]:
    """
    Load the facility CSV file, clean NULLs, and return as a list of dicts.
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

    logger.info("Loaded %d rows × %d columns from CSV directly into memory.", len(pdf), len(pdf.columns))

    # Convert the cleaned pandas dataframe to a list of dicts
    # .where(pd.notnull(pdf), None) ensures NaNs become Python None
    return pdf.where(pd.notnull(pdf), None).to_dict('records')
