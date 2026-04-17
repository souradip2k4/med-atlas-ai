"""
preprocessor.py — Synthesise a single structured text block from a CSV row.
"""

import re
import ast
import json
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


# Columns relevant to Step 1 (org classification) — REMOVED, org_type comes from CSV

# Columns relevant to Step 2 (fact validation + name cleaning + description generation)
_FACT_EXTRACTION_COLS = {
    "name", "name_variants", "description",
    "specialties", "procedure", "equipment", "capability",
}


def synthesize_for_org_classification(_row: Dict[str, Any]) -> str:
    """Legacy stub — Step 1 removed. Returns empty string."""
    return ""


def synthesize_for_fact_extraction(row: Dict[str, Any]) -> str:
    """Build the validation context text passed to the LLM.

    Presents the pre-merged data in a structured way for the LLM to review
    and clean — not to extract new facts.
    """
    lines: list[str] = []

    # ── Name variants ──────────────────────────────────────────────────────
    name_variants = row.get("name_variants") or []
    if isinstance(name_variants, str):
        try:
            name_variants = json.loads(name_variants)
        except (ValueError, TypeError):
            name_variants = [name_variants]
    if name_variants:
        lines.append(f"Name Variants (pick and clean the most accurate): {', '.join(repr(n) for n in name_variants)}")
    elif row.get("name"):
        lines.append(f"Name: {row['name']}")

    # ── Clinical arrays ────────────────────────────────────────────────────
    for col, label in [
        ("specialties", "Specialties"),
        ("procedure", "Procedures"),
        ("equipment", "Equipment"),
        ("capability", "Capabilities"),
    ]:
        value = row.get(col)
        str_val = str(value).strip() if value is not None else ""
        if not str_val or str_val.lower() in ("null", "none", "[]", '""'):
            lines.append(f"{label}: (none provided)")
            continue
        # Parse and render as bullet list
        if str_val.startswith("[") and str_val.endswith("]"):
            try:
                parsed = ast.literal_eval(str_val)
                if isinstance(parsed, list):
                    valid_items = [_normalise(str(x)) for x in parsed if x and str(x).strip()]
                    if valid_items:
                        lines.append(f"{label}:\n" + "\n".join(f"  - {x}" for x in valid_items))
                    else:
                        lines.append(f"{label}: (none provided)")
                    continue
            except (ValueError, SyntaxError):
                pass
        lines.append(f"{label}: {_normalise(str_val)}")

    # ── Description (for context only) ────────────────────────────────────
    desc = row.get("description")
    if desc and str(desc).strip() and str(desc).strip().lower() not in ("null", "none"):
        lines.append(f"Existing Description: {_normalise(str(desc))}")

    return "\n".join(lines)
