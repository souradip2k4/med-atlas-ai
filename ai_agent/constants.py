# ─── Domain constants ──────────────────────────────────────────────────────────

GHANA_REGIONS: list[str] = [
    "Ahafo", "Greater Accra", "Western", "Eastern", "Ashanti",
    "Volta", "Central", "Bono East", "Northern", "Western North",
    "Oti", "Bono", "North East", "Savannah", "Upper West", "Upper East",
]

# Critical life-saving procedures used as the VS query in cold-spot analysis.
# Covers high-burden interventions most frequently absent in low-resource settings.
CRITICAL_PROCEDURES: list[str] = [
    "caesarean section",
    "blood transfusion",
    "open heart surgeries",
    "kidney transplant surgeries",
    "renal dialysis treatment",
    "cataract surgery",
    "cornea transplant",
    "vitrectomy",
    "obstetric fistula repair",
    "laparotomy for ectopic gestations",
    "endoscopic retrograde cholangiopancreatography (ERCP)",
    "glaucoma surgeries",
    "general surgery",
    "safe abortion and post-abortion care",
    "anaesthesia services",
]

# Single VS query string that finds facilities offering ANY critical procedure.
_COLD_SPOT_PROCEDURE_QUERY: str = " ".join(CRITICAL_PROCEDURES)

# Human-message keywords that signal a cold-spot (ABSENCE) query vs. a standard
# GEO+SEMANTIC (PRESENCE) query. Checked by the postprocessor.
_COLD_SPOT_KEYWORDS: frozenset[str] = frozenset([
    "cold spot", "cold-spot", "coldspot",
    "absent", "absence", "lacking",
    "no access", "coverage gap",
    "procedure missing", "service absent",
    "travel time", "hours away",
    "where is", "where are there no",
])