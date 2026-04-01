from pydantic import BaseModel, Field
from typing import List, Any, Optional

class Message(BaseModel):
    role: str = Field(..., description="'system' or 'user'")
    content: str

class InvokeRequest(BaseModel):
    messages: List[Message] = Field(..., description="Conversation messages")
    user_id: Optional[str] = Field(default="api-user", description="User identifier for tracing")

class InvokeResponse(BaseModel):
    output: List[dict[str, Any]]
    citations: Optional[dict[str, Any]] = Field(
        default=None,
        description="Citation data keyed by agentic step."
    )
    agent: str = "MedAtlasAgent"
    endpoint: str = "fastapi"

class HealthResponse(BaseModel):
    status: str
    agent: str
    llm_endpoint: str
    tools: List[str]
