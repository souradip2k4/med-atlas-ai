from typing import Any
import json
from fastapi import APIRouter, HTTPException

from ai_agent.agent import AGENT, ALL_TOOLS, LLM_ENDPOINT
from mlflow.types.responses import ResponsesAgentRequest
from mlflow.types.agent import ChatContext

from ai_agent.api.schemas.agent import InvokeRequest, InvokeResponse, HealthResponse

router = APIRouter(tags=["Agent"])

@router.get("/health", response_model=HealthResponse)
def health():
    """Health check."""
    return HealthResponse(
        status="ok",
        agent="MedAtlasAgent",
        llm_endpoint=LLM_ENDPOINT,
        tools=[t.name for t in ALL_TOOLS],
    )

@router.get("/tools")
def list_tools():
    """List all available tools."""
    return {
        "tools": [
            {
                "name": t.name,
                "description": t.description,
                "parameters": t.args_schema.schema() if t.args_schema else {},
            }
            for t in ALL_TOOLS
        ]
    }

def _format_output_item(out: Any) -> dict[str, Any]:
    """Format an agent output item."""
    item: dict[str, Any] = {"type": getattr(out, "type", "unknown")}

    if item["type"] == "message":
        content_list = getattr(out, "content", None)
        if content_list:
            texts = []
            for c in content_list:
                if isinstance(c, dict):
                    texts.append(c.get("text", ""))
                else:
                    texts.append(getattr(c, "text", str(c)) if hasattr(c, "text") else str(c))
            item["content"] = "\n".join(texts)
        else:
            item["content"] = getattr(out, "text", "")
        item["role"] = getattr(out, "role", "assistant")
        return item

    if item["type"] == "function_call":
        item["tool_name"] = getattr(out, "name", "unknown")
        item["call_id"] = getattr(out, "call_id", "")
        args = getattr(out, "arguments", "")
        if isinstance(args, str):
            try:
                parsed = json.loads(args)
                item["arguments"] = json.dumps(parsed, indent=2)
            except Exception:
                item["arguments"] = args
        else:
            item["arguments"] = str(args)
        return item

    if item["type"] == "function_call_output":
        item["call_id"] = getattr(out, "call_id", "")
        item["tool_name"] = ""
        raw_output = getattr(out, "output", "")
        if isinstance(raw_output, str):
            try:
                parsed = json.loads(raw_output)
                item["output"] = json.dumps(parsed, indent=2)
                item["output_format"] = "json"
            except Exception:
                item["output"] = raw_output
                item["output_format"] = "text"
        else:
            item["output"] = str(raw_output)
            item["output_format"] = "text"
        return item

    return item

@router.post("/invoke", response_model=InvokeResponse)
def invoke(request: InvokeRequest):
    """Invoke the MedAtlasAgent."""
    try:
        req = ResponsesAgentRequest(
            input=[m.model_dump() for m in request.messages],
            context=ChatContext(user_id=request.user_id or "api-user"),
        )
        result = AGENT.predict_with_citations(req)
        output_items = [_format_output_item(out) for out in result.response.output]
        return InvokeResponse(output=output_items, citations=result.citations)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
