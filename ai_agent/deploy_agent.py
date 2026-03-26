"""
Med-Atlas-AI — FastAPI REST API Server
======================================
Wraps the MedAtlasAgent for REST access. MLflow integration is preserved
so agent traces are captured for evaluation.

Run locally:
  uv run uvicorn ai_agent.deploy_agent:app --reload --port 8000

Endpoints:
  POST /invoke   — call the agent (same as AGENT.predict in test_agent.py)
  GET  /health   — health check
  GET  /tools    — list available tools
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Same pattern as test_agent.py: import AGENT and ResponsesAgentRequest
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


# ── FastAPI app ───────────────────────────────────────────────────────────────

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


@app.post("/invoke", response_model=InvokeResponse)
def invoke(request: InvokeRequest):
    """
    Invoke the MedAtlasAgent.

    Mirrors the test_agent.py pattern:
      req = ResponsesAgentRequest(input=[...], context=ChatContext(...))
      resp = AGENT.predict(req)

    Example:
      {
        "messages": [{"role": "user", "content": "How many hospitals in Ashanti?"}]
      }
    """
    try:
        # Build ResponsesAgentRequest — same as test_agent.py
        req = ResponsesAgentRequest(
            input=[m.model_dump() for m in request.messages],
            context=ChatContext(user_id=request.user_id or "api-user"),
        )

        # Call agent — same as test_agent.py
        resp = AGENT.predict(req)

        # Extract output items from ResponsesResponse.output
        output_items = []
        for out in resp.output:
            item = {"type": getattr(out, "type", "unknown")}
            # Extract content from the nested content list if present
            if hasattr(out, "content") and out.content:
                texts = []
                for c in out.content:
                    if hasattr(c, "text"):
                        texts.append(c.text)
                    elif isinstance(c, dict) and "text" in c:
                        texts.append(c["text"])
                if texts:
                    item["content"] = "\n".join(texts)
            elif hasattr(out, "content") and not out.content:
                item["content"] = ""
            output_items.append(item)

        return InvokeResponse(output=output_items)

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Run locally ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "ai_agent.deploy_agent:app",
        host="0.0.0.0",
        port=port,
        reload=False,
    )
