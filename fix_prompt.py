import re

with open("ai_agent/agent.py", "r") as f:
    content = f.read()

injection = """### Step 2.5 — Medical Agent Tool Branch Selection Guide (CRITICAL):

The `medical_agent_tool` is powered by a backend SQL function that uses EXACT keyword matching (`RLIKE`) on your `query` argument to decide which analysis branch to run. **If you do not include specific keywords, your query may fail or hit the wrong branch!**

When calling `medical_agent_tool`, you MUST include one of the Exact Match Keywords in your `query` parameter depending on your goal:

| Backend Branch | Use When User Asks About... | MUST include at least one exact keyword in `query` |
|---|---|---|
| **Branch 1: Unmet Needs** | Missing specialties or absent procedures in a region | `unmet`, `gap`, `need`, `service gap` |
| **Branch 2: Capacity Outliers** | Unusually high/low bed or doctor numbers | `outlier`, `anomal`, `flag`, `capacity outlier`, `doctor anomaly` |
| **Branch 3: NGO Overlap** | Multiple NGOs operating similarly in the same city | `ngo overlap`, `overlapping ngo`, `same ngo`, `same region` |
| **Branch 4: Problem Type** | Facilities lacking all data for equipment or procedures | `problem type`, `root cause`, `gap type`, `classify gap`, `staff shortage` |
| **Branch 5: Deep Validation** | Verifying claims, procedure vs. equipment mismatches | `deep valid`, `validate`, `consistency`, `verify claim`, `mismatch`, `feature mismatch`, `procedure count`, `infrastr` |

*Example:* If the user asks "Find hospitals making suspicious surgical claims", DO NOT just use `"suspicious surgical claims"`. You must inject a Branch 5 keyword: `"verify claim for suspicious surgical claims"`.

"""

target = "### Step 2.5 — Anomaly Classification Protocol (applies after calling medical_agent_tool):"

if target in content and "Medical Agent Tool Branch Selection Guide" not in content:
    content = content.replace(target, injection + target)
    with open("ai_agent/agent.py", "w") as f:
        f.write(content)
        print("Successfully injected.")
else:
    print("Injection skipped or target not found.")

