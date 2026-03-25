import os
from pathlib import Path
from dotenv import load_dotenv

_dot = Path(".env")
load_dotenv(_dot)

from databricks_langchain import GenieAgent
GENIE_ID = os.environ["GENIE_SPACE_ID"]

print("\n--- Testing GenieAgent ---")
try:
    agent = GenieAgent(genie_space_id=GENIE_ID, return_pandas=False)
    print("Testing string invoke...")
    res = agent.invoke({"messages": [{"role": "user", "content": "How many hospitals?"}]})
    print("RES:", type(res))
except Exception as e:
    import traceback
    traceback.print_exc()

from databricks_langchain import VectorSearchRetrieverTool
VS_INDEX = os.environ.get("VS_INDEX", "med_atlas_ai.default.facility_facts_index")

print("\n--- Testing VectorSearchRetrieverTool ---")
try:
    vs = VectorSearchRetrieverTool(
        index_name=VS_INDEX,
        num_results=10
    )
    print("Testing string invoke...")
    res2 = vs.invoke({"query": "cardiac surgery"})
    print("RES2 dict:", type(res2), res2)
    
    res3 = vs.invoke("cardiac surgery")
    print("RES3 str:", type(res3), res3)
except Exception as e:
    import traceback
    traceback.print_exc()
