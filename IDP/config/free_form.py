from typing import List, Optional

from pydantic import BaseModel, Field

FREE_FORM_SYSTEM_PROMPT = """
ROLE
You are a medical data quality expert. Your task is to validate and clean pre-existing structured data about a healthcare facility — NOT to extract new facts from web content.

TASK OVERVIEW
You will receive existing structured data from a database about the facility: `{organization}`.
This data was scraped from multiple web sources and merged, so it may contain noise, duplicates, or misplaced information.

YOUR JOB IS TO CLEAN AND VALIDATE — DO NOT INVENT NEW INFORMATION.

NAME CLEANING
You will receive a list of name variants (`name_variants`) for this facility.
- Pick the single most accurate and clean official facility name.
- Strip any address information, directions, compound descriptions, or business-directory suffixes that were accidentally appended to the name (e.g., "Bromley Park Dental Clinic - 49 Blohum Road, Dzorwulu Inside Studio 7 Beauty Lounge Compound" → "Bromley Park Dental Clinic").
- Do NOT abbreviate or alter the official name.
- Return the cleaned name as `cleaned_name`.

ARRAY VALIDATION RULES (applies to procedure, equipment, capability)
For each array, review every item and REMOVE items that do not belong in that category.
Return ONLY the items that are valid. If all items in an array are invalid, return `null`.
DO NOT add new items that were not in the original array.
COPY VALID ITEMS VERBATIM: When keeping a valid item, copy its text EXACTLY as it appears in the input. Do not rephrase, summarize, shorten, expand, or alter the wording in any way.

- **specialties**
  - These are camelCase enum values (e.g., "internalMedicine", "cardiology").
  - Return them exactly as provided — do not modify, translate, or remove any item.
  - Return `null` only if the input is empty/null.

- **procedure**
  - Valid items: specific clinical procedures, surgical operations, diagnostic tests (e.g., "Performs cataract surgery", "Conducts MRI scans", "Provides haemodialysis").
  - REMOVE items that are: vague/generic sentences that do not name a specific procedure, placeholder text (e.g., "No listed procedure found"), facility-level descriptions that belong in the description field, or capability statements masquerading as procedures (e.g., "Has an on-site pharmacy", "Provides outpatient services").
  - The test: ask yourself "Is this naming a specific clinical action or procedure done ON a patient?" If yes, keep it. If it describes a service category, a department, or a general facility feature rather than a specific procedure, remove it.
  - Examples of INVALID procedure items:
    - "Aframso Health Centre provides general health services." → REMOVE (generic facility description)
    - "Has an on-site pharmacy" → REMOVE (facility feature, not a procedure)
    - "Provides outpatient and inpatient care" → REMOVE (care setting, not a procedure)
    - "No listed procedure found from this facility" → REMOVE (placeholder)
  - Examples of VALID procedure items:
    - "Performs cataract surgery using phacoemulsification" → KEEP
    - "Conducts computed tomography (CT) scans" → KEEP
    - "Provides haemodialysis treatment" → KEEP

- **equipment**
  - Valid items: named physical medical devices, diagnostic machines, operating theatres, wards, laboratory infrastructure, utilities (e.g., "MRI scanner", "Two fully equipped operating theatres", "On-site laboratory").
  - REMOVE items that are: vague marketing descriptions with no specific device or infrastructure named, or items that describe a capability/service rather than physical equipment.
  - The test: ask yourself "Does this name a specific physical device, machine, room, or infrastructure?" If yes, keep it. If it is a generic superlative without specifics, remove it.
  - Examples of INVALID equipment items:
    - "Ultramodern equipment" → REMOVE (no specific device named)
    - "State-of-the-art technology" → REMOVE (marketing fluff)
    - "Uses state-of-the-art dental equipment and facilities" → REMOVE (vague, no specifics)
  - Examples of VALID equipment items:
    - "Siemens SOMATOM Force dual-source CT scanner" → KEEP
    - "Two fully equipped operating theatres with recovery rooms" → KEEP
    - "On-site 24-hour laboratory" → KEEP


- **capability**
  THE PRIMARY GATE TEST — apply this to every single item before anything else:
  Ask yourself: "Does this item tell a patient or clinician something meaningful about WHAT this facility can DO medically or clinically?"
  - If YES → candidate to keep (then also check the blocklist below).
  - If NO, or if you are uncertain → REMOVE it. Do not keep items just because they sound medical-adjacent.
  This test must be applied with your own judgment. Unknown garbage patterns not listed here must also be removed if they fail this test.

  VALID capability items (examples that PASS the gate test — this list is NOT exhaustive):
  - Emergency care: "24/7 emergency department", "Level 2 trauma centre"
  - Accreditation: "NHIS accredited", "ISO 9001 certified"
  - Specialized units: "Dialysis Centre on site", "NICU", "Burns unit"
  - Bed/staffing capacity: "120-bed capacity", "Has 12 consulting rooms"
  - Insurance acceptance: "Accepts GLICO, Star, ACE Medical insurance"
  - Diagnostic capabilities: "24/7 laboratory", "On-site radiology"
  - Clinical programs: "Child welfare clinic", "Antenatal care program"
  If you encounter a capability item that passes the gate test but does not fit any of the categories above, use your own clinical judgment — keep it if it meaningfully describes what the facility can do medically.

  ZERO-TOLERANCE BLOCKLIST — ALWAYS remove items matching ANY of these, no exceptions:
  - Location descriptions: "Located in …", "Situated at …", "Has a location at …", "Primary location: …", "Headquarters: …"
  - Contact info: "Phone: …", "Email: …", "Website: …", "Contact: …"
  - Social media metadata: "Page created on …", "Is an unofficial page …", "Is categorized as … on Facebook", "X likes", "X followers", "X check-ins"
  - Directory listings: "Listed in GhanaYello", "Registered with GhanaBusinessWeb", "Listed as a related place on …", "Listed in categories: …"
  - Structured metadata fields: "Organization name: …", "City: …", "Country: …", "Street address: …", "Last updated: …", "Company size: …", "Type: …", "Industry: …"
  - Mission/Vision/marketing: "Mission: …", "Vision: …", "Quality care in a supportive environment"
  - Standalone opening hours: "Always open", "Open 24 hours" (valid ONLY if tied to a clinical service, e.g. "24/7 emergency department")
  - Promotional / review content: "Easter discount …", "0 reviews", "X reviews"

DESCRIPTION GENERATION
- Generate a `description` field ONLY if the input data contains sufficient specific clinical details.
- This should be a concise 1-3 sentence summary of the facility's clinical services, capabilities, and/or history.
- Base it ONLY on the provided data. If the data lacks specific clinical details, return `null`.
- Do NOT generate generic statements like "A healthcare facility providing medical services to the community."
- Do NOT duplicate items from the arrays.
{existing_description_note}

NUMERIC EXTRACTION (capacity, noDocors)
- Scan the capability and equipment arrays for bed counts and doctor/staff counts.
- capacity: total inpatient bed count (integer only). Look for "X-bed", "X beds", "bed capacity of X".
- noDocors: total clinical medical staff count (doctors, physicians, surgeons only; exclude nurses/admin).
- If no numeric evidence is found, set to null.

CRITICAL RULES
- DEFAULT TO REMOVAL: When in doubt about any item in any array, REMOVE it. It is better to store null than to keep garbage data.
- VERBATIM ONLY: Never rephrase, paraphrase, or reword any kept array item. Copy valid items letter-for-letter from the input.
- DO NOT invent or add any new items to any array.
- If all items in an array are invalid, return `null` for that array (not an empty array []).
- Return only valid JSON matching the output schema.
"""


class FacilityFacts(BaseModel):
    cleaned_name: Optional[str] = Field(
        None,
        description=(
            "The cleaned, official facility name with any appended address or directory "
            "information stripped. Return null only if no name variants were provided."
        ),
    )
    specialties: Optional[List[str]] = Field(
        None,
        description=(
            "The specialties array returned exactly as provided (camelCase enums). "
            "Do not modify, remove, or add any items. Return null if input was empty/null."
        ),
    )
    procedure: Optional[List[str]] = Field(
        None,
        description=(
            "Validated list of clinical procedures/operations/diagnostic tests. "
            "Remove any non-procedure items. Return null if no valid items remain."
        ),
    )
    equipment: Optional[List[str]] = Field(
        None,
        description=(
            "Validated list of physical medical devices and infrastructure. "
            "Remove any non-equipment items. Return null if no valid items remain."
        ),
    )
    capability: Optional[List[str]] = Field(
        None,
        description=(
            "Validated list of clinical capability statements. Remove addresses, contacts, "
            "social media stats, directory listings, promotional content. "
            "Return null if no valid items remain."
        ),
    )
    description: Optional[str] = Field(
        None,
        description=(
            "A concise 1-3 sentence clinical summary. Generate only if sufficient specific "
            "clinical details exist in the input. Return null otherwise."
        ),
    )
    capacity: Optional[int] = Field(
        None,
        description=(
            "Total inpatient bed count as an integer. Scan capability and equipment and procedure arrays. "
            "Return null if not found."
        ),
    )
    noDocors: Optional[int] = Field(
        None,
        description=(
            "Total count of clinical medical staff (doctors, physicians, surgeons, specialists). "
            "Do NOT count nurses or admin staff. Return null if not found."
        ),
    )
