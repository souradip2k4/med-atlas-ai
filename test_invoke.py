from ai_agent.api.routes.agent import invoke
from ai_agent.api.schemas.agent import InvokeRequest, Message

req = InvokeRequest(messages=[Message(role="user", content="Which facilities claim an unrealistic number of procedures relative to their size in Western Region?")])
try:
    invoke(req)
except Exception as e:
    import traceback
    traceback.print_exc()
