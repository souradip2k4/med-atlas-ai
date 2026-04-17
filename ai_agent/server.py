"""
Med-Atlas-AI — FastAPI REST API Server
======================================
Wraps the MedAtlasAgent and Map endpoints for REST access.

Run locally:
  uv run uvicorn ai_agent.server:app --reload --port 8000
"""

# Re-export the FastAPI app so `uvicorn ai_agent.server:app` continues to work
from ai_agent.api.main import app  # noqa: F401

if __name__ == "__main__":
    import uvicorn
    import os

    port = int(os.environ.get("PORT", 8000))
    # Note: We pass the module path to uvicorn so reload works cleanly if enabled
    uvicorn.run(
        "ai_agent.api.main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
    )
