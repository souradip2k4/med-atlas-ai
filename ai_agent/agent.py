"""
Med-Atlas-AI LangGraph Agent
============================
A healthcare infrastructure Q&A agent for Ghanaian medical facilities.

Tools:
  1. genie_chat_tool    — Natural language → SQL via Genie Space
  2. vector_search_tool — Semantic search on facility_facts (VS with fact_type filter)
  3. medical_agent_tool  — Anomaly detection via system.ai.python_exec UC function

Architecture:
  - Single LangGraph graph: [agent] → [tools] → [agent]
  - LLM decides which tool(s) to call based on query type
  - ResponsesAgent pattern for MLflow deployment compatibility
"""

# from __future__ import annotations

import json
import mlflow
import os
from pathlib import Path
from typing import Annotated, Generator, Sequence, TypedDict

# LangGraph
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_node import ToolNode

# LangChain
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.tools import tool

# MLflow ResponsesAgent
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    output_to_responses_items_stream,
    to_chat_completions_input,
)

# Databricks integrations
from databricks_langchain import ChatDatabricks

# ─── Configuration ─────────────────────────────────────────────────────────────

LLM_ENDPOINT  = os.environ["LLM_ENDPOINT"]
VS_INDEX     = os.environ.get("VS_INDEX", "med_atlas_ai.default.facility_facts_index")
GENIE_ID     = os.environ["GENIE_SPACE_ID"]
CATALOG      = os.environ.get("CATALOG", "med_atlas_ai")
SCHEMA       = os.environ.get("SCHEMA", "default")


# ─── Tool 1 — Genie Chat ──────────────────────────────────────────────────────

@tool
def genie_chat_tool(query: str) -> str:
    """
    Route quantitative, aggregation, and SQL-friendly questions to the Genie Space.

    Best for: facility counts, region/district statistics, averages, rankings,
    "how many", "total", "most", "top N", structured column filtering.
    NOT for: semantic similarity, free-text capability searches.
    """
    from databricks_langchain import GenieAgent

    agent = GenieAgent(
        genie_space_id=GENIE_ID,
        return_pandas=False,
    )
    return agent.invoke({"messages": [{"role": "user", "content": query}]})


# ─── Tool 2 — Vector Search ───────────────────────────────────────────────────

@tool
def vector_search_tool(query: str, fact_types: list[str] | str | None = None) -> str:
    """
    Semantic search over pre-generated facility facts.

    Best for: "Which facilities provide cardiac surgery?", "has MRI?",
    "similar to [name]", specialized services, capabilities, equipment.

    Args:
        query:      Natural language search query
        fact_types: Optional filter to specific fact types.
                    Valid values: procedure, capability, specialty, summary, equipment.
                    Pass a list like ["procedure"] or a single string like "procedure".
                    If None, searches across all fact types.
    """
    from databricks_langchain import VectorSearchRetrieverTool

    # Coerce str → list so LLM can pass either format without error
    if isinstance(fact_types, str):
        fact_types = [fact_types]

    # Only include columns that exist in the index
    kwargs = {
        "index_name": VS_INDEX,
        "num_results": 15,
        "columns": ["fact_id", "facility_id", "fact_text", "fact_type", "source_text"],
    }

    # Standard endpoint filter syntax: flat equality dict (not $in)
    # Use the first fact_type if provided; for multi-type, no filter is applied
    if fact_types and len(fact_types) == 1:
        kwargs["filters"] = {"fact_type": fact_types[0]}
    # For multiple fact_types: no server-side filter (all types searched), then post-filter
    post_filter_types = set(fact_types) if fact_types and len(fact_types) > 1 else None

    try:
        vs = VectorSearchRetrieverTool(**kwargs)
        results = vs.invoke({"query": query})
        # Post-filter if multiple fact_types were requested
        if post_filter_types and isinstance(results, list):
            results = [
                doc for doc in results
                if doc.metadata.get("fact_type") in post_filter_types
            ]
        return results
    except Exception as exc:
        return f"[Vector Search Error] {exc}"


# ─── Tool 3 — Medical Agent ───────────────────────────────────────────────────

@tool
def medical_agent_tool(query: str, facility_id: str | None = None) -> str:
    """
    Medical domain reasoning and anomaly detection on facility data.

    Uses system.ai.python_exec (built-in UC function) to run a Python rule
    engine on facility_records and facility_facts tables.

    Detects 8 anomaly types:
      1. equipment_procedure_mismatch  — MRI without radiology staff, etc.
      2. over_claiming               — clinic claiming cardiac surgery
      3. duplicate_facility          — same name in multiple records
      4. contradictory_signals       — conflicting ICU level statements
      5. ngo_classification          — direct_operator / supporter / none
      6. reliability_score           — 0-100 data quality score
      7. unmet_needs                 — service gaps by region
      8. anomaly_flagging             — outlier bed/doctor counts

    Args:
        query:      Analysis question (e.g., "detect anomalies", "score reliability")
        facility_id: Optional. Restrict analysis to one facility.

    Returns: Structured JSON with findings, severity, and recommendations.
    """
    from databricks_langchain import UCFunctionToolkit

    CAT = CATALOG
    SCH = SCHEMA

    # Call the SQL UC function (works on PRO warehouses, no PySpark needed).
    # The UC function returns JSON where findings is a JSON-string-encoded array.
    args = {"query": query}
    if facility_id:
        args["facility_id"] = facility_id

    try:
        uc = UCFunctionToolkit(
            function_names=[f"{CAT}.{SCH}.analyze_medical_query"]
        )
        uc_fn = uc.tools[0]
        raw_result = uc_fn.invoke({"query_json": json.dumps(args)})
        # The UC function returns a JSON string; parse it and decode findings
        outer = json.loads(raw_result)
        findings_raw = outer.get("findings", "[]")
        findings = json.loads(findings_raw)
        outer["findings"] = findings
        return json.dumps(outer, indent=2)
    except Exception as exc:
        return f"[Medical Agent Error] {exc}"


# ─── Tool list ────────────────────────────────────────────────────────────────

ALL_TOOLS = [genie_chat_tool, vector_search_tool, medical_agent_tool]


# ─── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are Med-Atlas-AI, a healthcare infrastructure analyst for Ghana.

Use exactly ONE tool per query:

• "how many", "count", "total", "average", "sum", "most", "least", "top N",
  "region", "district", "ownership", "beds", "staff" → genie_chat_tool

• "similar", "like", "capability", "service", "equipment", "provides",
  "specialty", "has", "can provide", "offers" → vector_search_tool

• "anomal", "inconsisten", "contradict", "reliab", "score", "quality",
  "ngo", "classify", "over-claim", "implausib", "mismatch", "gap",
  "unmet need", "equipment", "outlier", "flag", "duplicate" → medical_agent_tool

For multi-part questions, call multiple tools. Cite facility names and regions.
Format tabular results as markdown. If a tool returns no results, say so clearly.
"""

# Count health centres in each region AND find anomalies.
# ─── LangGraph ────────────────────────────────────────────────────────────────

class AgentState(TypedDict):
    messages: Annotated[Sequence, add_messages]


llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1, max_tokens=2048)
llm_with_tools = llm.bind_tools(ALL_TOOLS)


def call_model(state: AgentState) -> dict:
    messages = [SystemMessage(content=SYSTEM_PROMPT)] + list(state["messages"])
    response = llm_with_tools.invoke(messages)
    return {"messages": [response]}


def should_continue(state: AgentState) -> str:
    last = state["messages"][-1]
    if isinstance(last, AIMessage) and last.tool_calls:
        return "tools"
    return "end"


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("agent", RunnableLambda(call_model))
    graph.add_node("tools", ToolNode(ALL_TOOLS))
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": END})
    graph.add_edge("tools", "agent")
    graph.set_entry_point("agent")
    return graph.compile()


# ─── ResponsesAgent (MLflow deployment interface) ──────────────────────────────

class MedAtlasAgent(ResponsesAgent):
    def __init__(self):
        self.graph = build_graph()

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        messages = to_chat_completions_input([m.model_dump() for m in request.input])
        for event in self.graph.stream({"messages": messages}, stream_mode=["updates"]):
            if event[0] == "updates":
                for node_data in event[1].values():
                    if isinstance(node_data, dict) and node_data.get("messages"):
                        yield from output_to_responses_items_stream(node_data["messages"])


# ─── Export ────────────────────────────────────────────────────────────────────

AGENT = MedAtlasAgent()
mlflow.models.set_model(AGENT)
