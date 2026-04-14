import re

with open("ai_agent/agent.py", "r") as f:
    content = f.read()

# 1. Update the Branch Selection Guide to embed the region requirement for Branch 5:
content = content.replace(
    "| **Branch 5: Deep Validation** | Verifying claims, procedure vs. equipment mismatches |",
    "| **Branch 5: Deep Validation** | Verifying claims/mismatches. *(Requires passing a `region` or `facility_name`!)* |"
)

# 2. Extract and delete the redundant Step 2.5 protocols
# We use regex to selectively remove the chunks from "### Step 2.5 — Medical Reasoning Protocol"
# all the way down to just before "### Step 2.5 — Medical Agent Tool Branch Selection Guide"

start_string = "### Step 2.5 — Medical Reasoning Protocol (applies when query involves medical domain judgment):"
end_string = "### Step 2.5 — Medical Agent Tool Branch Selection Guide (CRITICAL):"

if start_string in content and end_string in content:
    start_idx = content.find(start_string)
    end_idx = content.find(end_string)
    # Remove everything in between!
    content = content[:start_idx] + content[end_idx:]
    print("Deleted 3 redundant protocols!")
else:
    print("Could not find bounds to delete the redundant protocols.")

# 3. Rewrite the Anomaly Classification Protocol for deep_validation
# From: "The tool has already performed batch LLM analysis internally..."
# To: Our new streamlined medical reasoning instructions.

old_deep_val = """  • For `deep_validation` (Specialty/Procedure/Equipment Consistency + Feature Mismatch):
      The tool has already performed batch LLM analysis internally. The results
      contain pre-analyzed `validation_results` with `status`, `severity`, `mismatches`,
      and `reasoning` for each facility. Present these grouped by severity:
      1. **ALWAYS start** with `data_coverage_summary` — state how many facilities
         were skipped due to insufficient data.
      2. List **high** severity mismatches first (these are the most concerning).
      3. Then **medium** and **low** severity.
      4. For facilities with `status: consistent`, briefly note they passed.
      5. Format as a clear markdown report with facility names and specific mismatches.
      6. Check #2 of the internal validator (PROCEDURE→EQUIPMENT) catches qualitative
         mismatches (e.g., Brain Surgery claimed with only a Thermometer) even when the
         numeric count ratio appears normal. Trust the `mismatches` and `reasoning` fields."""

new_deep_val = """  • For `deep_validation` (Verifying claims and capabilities):
      The tool returns raw structured lists of claimed specialties, procedures, and equipment. You MUST use your own medical expertise to analyze them for contradictions and plausibility mismatches:
      1. **ALWAYS start** with `data_coverage_summary` — state how many facilities were skipped due to insufficient data.
      2. **Apply Medical Reasoning:**
         → Check PROCEDURE→EQUIPMENT consistency (e.g., Brain Surgery claimed with only a Thermometer).
         → Check SPECIALTY→PROCEDURE consistency.
         → Check FACILITY_TYPE and CAPACITY realism (e.g., neurosurgery at a small clinic).
      3. List **high** severity mismatches first (these are the most concerning), then medium/low.
      4. For facilities with no detectable mismatches, briefly note they appear consistent."""

if old_deep_val in content:
    content = content.replace(old_deep_val, new_deep_val)
    print("Successfully replaced deep_validation rule.")
else:
    print("Warning: old deep_validation string not found exactly.")


with open("ai_agent/agent.py", "w") as f:
    f.write(content)
