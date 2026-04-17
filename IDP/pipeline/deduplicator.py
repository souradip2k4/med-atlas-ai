"""
deduplicator.py — Pre-LLM deduplication of CSV rows by pk_unique_id.

Groups all rows sharing the same pk_unique_id into a single consolidated row:
  - Array columns (specialties, procedure, equipment, capability): union-merge, deduplicate
  - Name: pick shortest non-null variant as primary; stash all variants in `name_variants`
  - Scalar columns: take the longest non-null string value across duplicates
  - Social links: first non-null value per platform across duplicates
  - IDs / logo: keep from first row
"""

import ast
import json
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Column categorisation ──────────────────────────────────────────────────

# These will be union-merged across duplicate rows and deduplicated
# Only columns whose CSV values are JSON-encoded arrays go here
_ARRAY_COLS = {
    "specialties", "procedure", "equipment", "capability",  # clinical arrays
    "phone_numbers", "websites",                            # contact arrays
}

# Kept from the first row; treated as stable identifiers
_ID_COLS = {
    "pk_unique_id", "unique_id", "mongo_db", "content_table_id",
    "source_url"
}

# Everything else → take the longest non-null string value
# (email, officialWebsite, yearEstablished, acceptsVolunteers,
#  facebookLink, twitterLink, linkedinLink, instagramLink, logo,
#  all address_* fields, countries, missionStatement, missionStatementLink,
#  organizationDescription, facilityTypeId, operatorTypeId, affiliationTypeIds,
#  description, area, numberDoctors, capacity, organization_type, etc.)


# ── Helpers ────────────────────────────────────────────────────────────────

def _parse_array(value: Any) -> List[str]:
    """Parse a JSON/Python array string or list into a flat list of strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if x is not None and str(x).strip()]
    s = str(value).strip()
    if not s or s.lower() in ("null", "none", "[]", '""'):
        return []
    # Try JSON first, then ast.literal_eval
    for parser in (json.loads, ast.literal_eval):
        try:
            result = parser(s)
            if isinstance(result, list):
                return [str(x).strip() for x in result if x is not None and str(x).strip()]
        except Exception:
            pass
    return []


def _merge_arrays_dedup(*arrays: List[str]) -> Optional[List[str]]:
    """Union-merge multiple string lists, deduplicated case-insensitively, order preserved."""
    seen: set[str] = set()
    merged: List[str] = []
    for arr in arrays:
        for item in arr:
            key = item.lower()
            if key and key not in seen:
                seen.add(key)
                merged.append(item)
    return merged if merged else None


def _longest_non_null(*values: Any) -> Optional[str]:
    """Return the longest non-null, non-empty string value from the candidates."""
    best: Optional[str] = None
    best_len = -1
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if not s or s.lower() in ("null", "none", "[]", '""'):
            continue
        if len(s) > best_len:
            best = s
            best_len = len(s)
    return best


def _shortest_non_null(*values: Any) -> Optional[str]:
    """Return the shortest non-null, non-empty string value from the candidates."""
    best: Optional[str] = None
    best_len = float("inf")
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if not s or s.lower() in ("null", "none"):
            continue
        if len(s) < best_len:
            best = s
            best_len = len(s)
    return best


# ── Main deduplication logic ───────────────────────────────────────────────

def deduplicate_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Group rows by pk_unique_id and merge duplicates into a single consolidated row.

    Parameters
    ----------
    rows : list of dict
        Raw CSV rows as returned by load_csv_data().

    Returns
    -------
    list of dict
        One consolidated dict per unique facility (pk_unique_id).
        Each dict gains an extra key ``name_variants`` (list of str) with all
        unique name strings found across duplicate rows.
    """
    # Group rows by pk_unique_id
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = str(row.get("pk_unique_id") or row.get("unique_id") or "")
        groups[key].append(row)

    consolidated: List[Dict[str, Any]] = []

    for pk_id, group in groups.items():
        if len(group) == 1:
            # No duplicates — just tag name_variants and move on
            row = dict(group[0])
            primary_name = row.get("name")
            primary_name = str(primary_name).strip().title() if primary_name else ""
            row["name"] = primary_name
            row["name_variants"] = [primary_name] if primary_name else []
            consolidated.append(row)
            continue

        logger.debug("Merging %d duplicate rows for pk_unique_id=%s", len(group), pk_id)

        # Start with the first row's ID fields as the base
        base = dict(group[0])

        # ── Name: collect all unique variants; primary = shortest ──────────
        all_names = []
        seen_names: set[str] = set()
        for r in group:
            n = r.get("name")
            if n and str(n).strip():
                clean_n = str(n).strip().title()
                if clean_n.lower() not in seen_names:
                    seen_names.add(clean_n.lower())
                    all_names.append(clean_n)
                    
        # Primary name = shortest (least likely to have address appended)
        primary_name = _shortest_non_null(*all_names) or ""
        base["name"] = primary_name
        base["name_variants"] = all_names  # all variants passed to LLM for name cleaning

        # ── Array columns: union-merge with dedup ───────────────────────────
        for col in _ARRAY_COLS:
            merged = _merge_arrays_dedup(*[_parse_array(r.get(col)) for r in group])
            base[col] = json.dumps(merged) if merged else None

        # ── Scalar columns: longest non-null string ─────────────────────────
        all_cols = set(base.keys())
        handled_cols = _ARRAY_COLS | _ID_COLS | {"name", "name_variants"}
        scalar_cols = all_cols - handled_cols

        for col in scalar_cols:
            values = [r.get(col) for r in group]
            base[col] = _longest_non_null(*values)

        base["source_row_count"] = len(group)
        consolidated.append(base)

    # ── Location String formatting (city & state) ──────────────────────────
    import re
    def _clean_loc(val: Any) -> Optional[str]:
        if not val:
            return None
        s = str(val).strip()
        if not s or s.lower() in ("null", "none"):
            return None
        s = re.sub(r'(?i)\bregion\b', '', s)
        s = re.sub(r'(?i)\bcity\b', '', s)
        s = ' '.join(s.split()).title()
        return s if s else None

    for row in consolidated:
        if "address_city" in row:
            row["address_city"] = _clean_loc(row["address_city"])
        if "address_stateorregion" in row:
            row["address_stateorregion"] = _clean_loc(row["address_stateorregion"])

    original_count = len(rows)
    deduped_count = len(consolidated)
    logger.info(
        "Deduplication complete: %d rows → %d unique facilities (%d duplicate rows removed)",
        original_count, deduped_count, original_count - deduped_count,
    )
    return consolidated
