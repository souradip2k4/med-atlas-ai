from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ai_agent.agent import ALL_TOOLS, LLM_ENDPOINT
from ai_agent.api.routes import agent, map

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("MedAtlasAgent FastAPI server starting...")
    print(f"  LLM endpoint: {LLM_ENDPOINT}")
    print(f"  Tools:        {[t.name for t in ALL_TOOLS]}")
    yield
    print("MedAtlasAgent FastAPI server shutting down.")

app = FastAPI(
    title="Med-Atlas-AI API",
    description="Healthcare infrastructure Q&A agent and Map backend for Ghanaian medical facilities.",
    version="1.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include the endpoints
app.include_router(agent.router)
app.include_router(map.router)
