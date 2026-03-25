import os
from pathlib import Path
from dotenv import load_dotenv

_dot = Path(".env")
load_dotenv(_dot)

from databricks_langchain import GenieAgent
GENIE_ID = os.environ["GENIE_SPACE_ID"]

try:
    agent = GenieAgent(genie_space_id=GENIE_ID, return_pandas=False)
    print("Testing string invoke...")
    res = agent.invoke({"messages": [{"role": "user", "content": "How many hospitals?"}]})
    print("RES:", type(res))
except Exception as e:
    import traceback
    traceback.print_exc()

