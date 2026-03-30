"""
preprocessor.py — Synthesise a single structured text block from a CSV row.
"""

import re
import ast
from typing import Dict, Any


# Columns that are identifiers / not useful for LLM context
_SKIP_COLS = {
    "pk_unique_id",
    "mongo_db",
    "content_table_id",
    "unique_id",
}


def _normalise(text: str) -> str:
    """Strip, collapse duplicate whitespace."""
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    return text


def synthesize_row_text(row: Dict[str, Any]) -> str:
    """
    Combine all non-null, non-empty fields of a row into a concise
    structured text block suitable for LLM consumption.

    Normalisation rules applied:
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

        # ── List Parsing ─────────────────────────────────────────────────────────
        if str_val.startswith("[") and str_val.endswith("]"):
            try:
                parsed_list = ast.literal_eval(str_val)
                if isinstance(parsed_list, list):
                    valid_items = [_normalise(str(x)) for x in parsed_list if x and str(x).strip()]
                    if valid_items:
                        normalised_val = "\n  - " + "\n  - ".join(valid_items)
                    else:
                        continue
                else:
                    normalised_val = _normalise(str_val)
            except (ValueError, SyntaxError):
                normalised_val = _normalise(str_val)
        else:
            normalised_val = _normalise(str_val)

        # Use a readable field label (replace underscores, title-case)
        label = key.replace("_", " ").title().strip()
        lines.append(f"{label}: {normalised_val}")

    return "\n".join(lines)


# ── Targeted synthesizers (Phase 2: reduced LLM scope) ───────────────────

# Columns relevant to Step 1 (org classification)
_ORG_CLASSIFICATION_COLS = {
    "name", "description", "organization_type",
}

# Columns relevant to Step 2 (fact extraction + description generation)
_FACT_EXTRACTION_COLS = {
    "name", "description",
    "specialties", "procedure", "equipment", "capability",
}


def synthesize_for_org_classification(row: Dict[str, Any]) -> str:
    """Build text from only the columns needed for org type classification."""
    filtered = {k: v for k, v in row.items() if k.lower() in _ORG_CLASSIFICATION_COLS}
    return synthesize_row_text(filtered)


def synthesize_for_fact_extraction(row: Dict[str, Any]) -> str:
    """Build text from only the columns needed for fact/description extraction."""
    filtered = {k: v for k, v in row.items() if k.lower() in _FACT_EXTRACTION_COLS}
    return synthesize_row_text(filtered)
