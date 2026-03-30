"""
Med-Atlas-AI — FastAPI REST API Server
======================================
Wraps the MedAtlasAgent for REST access. MLflow integration is preserved
so agent traces are captured for evaluation.

Run locally:
  uv run uvicorn ai_agent.server:app --reload --port 8000

Endpoints:
  POST /invoke   — call the agent (same as AGENT.predict in test_agent.py)
  GET  /health   — health check
  GET  /tools    — list available tools
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any
import json

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from ai_agent.agent import AGENT, ALL_TOOLS, LLM_ENDPOINT
from mlflow.types.responses import ResponsesAgentRequest
from mlflow.types.agent import ChatContext


# ── Pydantic request/response models ──────────────────────────────────────────

class Message(BaseModel):
    role: str = Field(..., description="'system' or 'user'")
    content: str


class InvokeRequest(BaseModel):
    messages: list[Message] = Field(..., description="Conversation messages")
    user_id: str | None = Field(default="api-user", description="User identifier for tracing")


class InvokeResponse(BaseModel):
    output: list[dict[str, Any]]
    citations: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Citation data keyed by agentic step. Contains:\n"
            "  steps[]    — one entry per tool call, ordered by execution\n"
            "  summary    — aggregate stats (facilities referenced, tables used)"
        ),
    )
    agent: str = "MedAtlasAgent"
    endpoint: str = "fastapi"


class HealthResponse(BaseModel):
    status: str
    agent: str
    llm_endpoint: str
    tools: list[str]


# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("MedAtlasAgent FastAPI server starting...")
    print(f"  LLM endpoint: {LLM_ENDPOINT}")
    print(f"  Tools:        {[t.name for t in ALL_TOOLS]}")
    yield
    print("MedAtlasAgent FastAPI server shutting down.")


# ── FastAPI app ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Med-Atlas-AI API",
    description="Healthcare infrastructure Q&A agent for Ghanaian medical facilities.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
def health():
    """Health check."""
    return HealthResponse(
        status="ok",
        agent="MedAtlasAgent",
        llm_endpoint=LLM_ENDPOINT,
        tools=[t.name for t in ALL_TOOLS],
    )


@app.get("/tools")
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
    """
    Format a single output item from the agent into a clean dict.

    Handles three types:
      - message       → {type, content}
      - function_call → {type, tool_name, arguments}
      - function_call_output → {type, tool_name, output}
    """
    item: dict[str, Any] = {"type": getattr(out, "type", "unknown")}

    # ── message ──────────────────────────────────────────────────────────────
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

    # ── function_call ─────────────────────────────────────────────────────────
    if item["type"] == "function_call":
        item["tool_name"] = getattr(out, "name", "unknown")
        item["call_id"] = getattr(out, "call_id", "")
        args = getattr(out, "arguments", "")
        # Pretty-print JSON arguments if they look like JSON
        if isinstance(args, str):
            try:
                parsed = json.loads(args)
                item["arguments"] = json.dumps(parsed, indent=2)
            except Exception:
                item["arguments"] = args
        else:
            item["arguments"] = str(args)
        return item

    # ── function_call_output ──────────────────────────────────────────────────
    if item["type"] == "function_call_output":
        item["call_id"] = getattr(out, "call_id", "")
        item["tool_name"] = ""  # Resolved below via call_id
        raw_output = getattr(out, "output", "")
        if isinstance(raw_output, str):
            # Try to parse as JSON for clean display
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

    # ── fallback ─────────────────────────────────────────────────────────────
    return item


@app.post("/invoke", response_model=InvokeResponse)
def invoke(request: InvokeRequest):
    """
    Invoke the MedAtlasAgent.

    Output structure:
      - function_call        → which tool was called and with what arguments
      - function_call_output → what the tool returned
      - message             → the final LLM response

    Example:
      {
        "messages": [{"role": "user", "content": "How many hospitals in Ashanti?"}]
      }
    """
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


# ── Run locally ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    import json as _json

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "ai_agent.deploy_agent:app",
        host="0.0.0.0",
        port=port,
        reload=False,
    )
