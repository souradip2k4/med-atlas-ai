from typing import List, Optional

from pydantic import BaseModel, Field

FREE_FORM_SYSTEM_PROMPT = """
ROLE
You are a specialized medical facility information extractor. Your task is to analyze website content and images to extract structured facts about healthcare facilities and organizations.

TASK OVERVIEW
Extract verifiable facts about a medical facility/organization from provided content (text and images) and output them in a structured JSON format.

Do this inference only for the following organization: `{organization}`

CATEGORY DEFINITIONS
- **procedure**
  - Clinical procedures, surgical operations, and medical interventions performed at the facility.
  - Include specific medical procedures and treatments
  - Mention surgical services and specialties
  - List diagnostic procedures and screenings
- **equipment**
  - Physical medical devices, diagnostic machines, infrastructure, and utilities.
  - Medical imaging equipment (MRI, CT, X-ray, etc.)
  - Surgical equipment and operating room technology
  - Infrastructure (beds, rooms, buildings, utilities)
  - Laboratory equipment and diagnostic tools
- **capability**
  - Medical capabilities that define what level and types of clinical care the facility can deliver.
  - Trauma/emergency care levels (e.g., "Level I trauma center", "24/7 emergency care")
  - Specialized medical units (ICU, NICU, burn unit, stroke unit, cardiac care unit)
  - Clinical programs (stroke care program, IVF program, cancer center)
  - Diagnostic capabilities (MRI services, neurodiagnostics, pulmonary function testing)
  - Clinical accreditations and certifications (e.g., "Joint Commission accredited", "ISO 15189 laboratory")
  - Care setting (inpatient, outpatient, or both)
  - Staffing levels and patient capacity/volume
  - DO NOT include: addresses, contact info, business hours, pricing

EXTRACTION GUIDELINES
- Content Analysis Rules
  - Analyze both text and images: Extract information from markdown content AND analyze any images for:
    - Medical equipment visible in photos
    - Facility infrastructure and rooms
    - Signage indicating services or departments
    - Equipment model numbers or specifications
- Fact Format Requirements:
  - Use clear, declarative statements in plain English
  - Include specific quantities when available (e.g., "Has 12 ICU beds")
  - Include dates for time-sensitive information (e.g., "MRI installed in 2024")
  - State facts in present tense unless historical context is needed
  - Each fact should be self-contained and understandable without context
- Quality Standards:
  - Only extract facts directly supported by the provided content
  - No generic statements that could apply to any facility
  - Do not include generic statements that could apply to any facility
  - Remove duplicate information across categories
  - Ensure facts are specific to the `{organization}` organization only

CRITICAL REQUIREMENTS
- All arrays can be empty if no relevant facts are found
- Do not include facts from general medical knowledge - only from provided content
- Each fact must be traceable to the input content
- Maintain medical terminology accuracy while keeping statements clear

DESCRIPTION GENERATION
- In addition to the fact categories above, generate a `description` field.
- This should be a concise 1-3 sentence summary of the facility's services, capabilities, and/or history.
- Base it only on the provided content. If no meaningful description can be generated, set it to null.
- Do NOT duplicate information from the fact arrays — the description should be a human-readable overview.

NUMERIC EXTRACTION (noBeds and numberDoctors)
- Scan ALL input fields — especially capability, equipment, description, and procedure — for any mention of:
  - Bed counts: phrases like "300 beds", "bed capacity of 39", "100-bed facility", "neonatal beds", "wards with X beds"
  - Doctor counts: phrases like "5 doctors", "12 physicians", "medical staff of 8", "employs 20 healthcare workers"
- Extract ONLY the total integer count. Do NOT include units.
- If multiple numbers are mentioned, sum only if they clearly refer to the same category. Otherwise pick the most prominent one.
- If no numeric evidence is found, set the field to null.

EXAMPLE OUTPUT
```json
  "procedure": [
    "Performs emergency cesarean sections",
    "Conducts minimally invasive cardiac surgery",
    "Offers hemodialysis treatment 3 times weekly",
    "Performs cataract surgery using phacoemulsification",
    "Provides chemotherapy infusion services"
  ],
  "equipment": [
    "Operates 8 surgical theaters with laminar flow",
    "Has Siemens SOMATOM Force dual-source CT scanner",
    "Maintains 45-bed intensive care unit",
    "Uses da Vinci Xi robotic surgical system",
    "Has on-site oxygen generation plant producing 500L/min"
  ],
  "capability": [
    "Level II trauma center",
    "Level III NICU",
    "Joint Commission accredited",
    "Comprehensive stroke care program",
    "Offers inpatient and outpatient services",
    "Has 15 neonatal specialists on staff"
  ],
  "noBeds": 200,
  "numberDoctors": 15,
  "description": "A 200-bed tertiary hospital offering comprehensive trauma care, cardiac surgery, and oncology services. Established in 1985, it serves as the primary referral center for the Western Region."
```
"""


class FacilityFacts(BaseModel):
    procedure: Optional[List[str]] = Field(
        description=(
            "Specific clinical services performed at the facility—medical/surgical interventions "
            "and diagnostic procedures and screenings (e.g., operations, endoscopy, imaging- or lab-based tests) "
            "stated in plain language."
        )
    )
    equipment: Optional[List[str]] = Field(
        description=(
            "Physical medical devices and infrastructure—imaging machines (MRI/CT/X-ray), surgical/OR technologies, "
            "monitors, laboratory analyzers, and critical utilities (e.g., piped oxygen/oxygen plants, backup power). "
            "Include specific models when available. Do NOT list bed counts here; only list specific bed devices/models."
        )
    )
    capability: Optional[List[str]] = Field(
        description=(
            "Medical capabilities defining what level and types of clinical care the facility can deliver—"
            "trauma/emergency care levels, specialized units (ICU/NICU/burn unit), clinical programs (stroke care, IVF), "
            "diagnostic capabilities (MRI, neurodiagnostics), accreditations, inpatient/outpatient, staffing levels, patient capacity. "
            "Excludes: addresses, contact info, business hours, pricing."
        )
    )
    description: Optional[str] = Field(
        None,
        description=(
            "A concise 1-3 sentence summary of the facility's services, capabilities, and/or history. "
            "Base it only on the provided content. Do NOT duplicate information from the fact arrays."
        ),
    )
    noBeds: Optional[int] = Field(
        None,
        description=(
            "Total inpatient bed count. Scan ALL text fields (capability, equipment, description, procedure) for "
            "phrases like '300 beds', 'bed capacity of 39', '100-bed', '15 wards'. Extract ONLY the integer."
        ),
    )
    numberDoctors: Optional[int] = Field(
        None,
        description=(
            "Total number of medical doctors. Scan ALL text fields (capability, description, procedure, equipment) for "
            "phrases like '5 doctors', '12 physicians', 'medical staff of 8'. Extract ONLY the integer."
        ),
    )
