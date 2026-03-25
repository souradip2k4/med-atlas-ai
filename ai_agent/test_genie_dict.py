import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(".env"))

from databricks_langchain import GenieAgent
GENIE_ID = os.environ["GENIE_SPACE_ID"]

try:
    agent = GenieAgent(genie_space_id=GENIE_ID, return_pandas=False)
    res = agent.invoke({"messages": [{"role": "user", "content": "How many hospitals?"}]})
    print("RES TYPE:", type(res))
    print("RES VALUE:", res)
except Exception as e:
    import traceback
    traceback.print_exc()
