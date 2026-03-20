"""
preprocessor.py — Synthesise a single structured text block from a CSV row.
"""

import re
from typing import Dict, Any


# Columns that are identifiers / not useful for LLM context
_SKIP_COLS = {
    "pk_unique_id",
    "mongo_db",
    "content_table_id",
    "unique_id",
}


def _normalise(text: str) -> str:
    """Lowercase, strip, collapse duplicate whitespace."""
    text = text.strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def synthesize_row_text(row: Dict[str, Any]) -> str:
    """
    Combine all non-null, non-empty fields of a row into a concise
    structured text block suitable for LLM consumption.

    Normalisation rules applied:
      - lowercase
      - strip whitespace
      - remove duplicate spaces

    Parameters
    ----------
    row : dict
        A single row from the raw_facilities table.

    Returns
    -------
    str
        A multi-line ``"field: value"`` text block.
    """
    lines: list[str] = []

    for key, value in row.items():
        if key in _SKIP_COLS:
            continue
        if value is None:
            continue

        str_val = str(value).strip()
        if not str_val or str_val.lower() in ("null", "none", "n/a", "[]", '""'):
            continue

        normalised_val = _normalise(str_val)
        # Use a readable field label (replace underscores, title-case)
        label = key.replace("_", " ").strip()
        lines.append(f"{label}: {normalised_val}")

    return "\n".join(lines)
