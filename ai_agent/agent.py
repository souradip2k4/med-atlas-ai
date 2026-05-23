"""
Med-Atlas-AI LangGraph Agent
============================
A healthcare infrastructure Q&A agent for Ghanaian medical facilities.

Tools:
  1. genie_chat_tool         — Natural language → SQL via Genie Space
  2. vector_search_tool      — Semantic search on facility_facts (VS with fact_type filter)
  3. medical_agent_tool      — Statistical anomaly detection via analyze_medical_query UC function
  4. geospatial_query_tool   — Distance-based facility search via find_facilities_nearby UC function

Architecture:
  - Single LangGraph graph: [agent] → [tools] → [tool_postprocessor] → [agent]
  - LLM decides which tool(s) to call based on query type
  - ResponsesAgent pattern for MLflow deployment compatibility
"""

from ai_agent.graph import build_graph
from ai_agent.citations import _ToolCallTracker
import json
import re
import uuid
import warnings
import mlflow
import os
from pathlib import Path
from typing import Annotated, Any, Generator, Sequence, TypedDict


# MLflow ResponsesAgent (MLflow 3.x)
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    ResponseOutputItemDoneEvent,
    OutputItem,
    to_chat_completions_input,
)
from mlflow.types.responses_helpers import (
    Content
)

# ─── Stream aggregator ─────────────────────────────────────────────────────────
#
# output_to_responses_items_stream() processes each message independently and
# emits function_call / function_call_output as separate events. For a
# readable tool-call sequence, we aggregate them and emit:
#
#   function_call → function_call_output → message
#
# in execution order. This lets API consumers see exactly which tools ran and
# in what sequence before the final answer.

# ─── ResponsesAgent (MLflow 3.x deployment interface) ──────────────────────────

from typing import NamedTuple


class _AgentResult(NamedTuple):
    """Internal result wrapper carrying both the response and citation data."""
    response: ResponsesAgentResponse
    citations: dict[str, Any]


class MedAtlasAgent(ResponsesAgent):
    def __init__(self):
        self.graph = build_graph()

    def _run_graph(
        self, request: ResponsesAgentRequest, tracker: _ToolCallTracker
    ) -> list[ResponsesAgentStreamEvent]:
        """Run the LangGraph, process all messages into tracker, return events."""
        messages = to_chat_completions_input([m.model_dump() for m in request.input])
        for event in self.graph.stream(
            {"messages": messages},
            config={"recursion_limit": 100},
            stream_mode=["updates"],
        ):
            if event[0] != "updates":
                continue
            for node_data in event[1].values():
                if not isinstance(node_data, dict) or not node_data.get("messages"):
                    continue
                for msg in node_data["messages"]:
                    tracker.process_message(msg)
        return tracker.finalize()

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        """MLflow-compatible predict (no citations)."""
        tracker = _ToolCallTracker()
        events = self._run_graph(request, tracker)
        outputs = [e.item for e in events if e.type == "response.output_item.done"]
        return ResponsesAgentResponse(output=outputs)

    def predict_with_citations(self, request: ResponsesAgentRequest) -> _AgentResult:
        """Run the agent and return both the response AND structured citations."""
        from mlflow.entities import SpanType

        # Extract user question for the MLflow UI
        user_input = ""
        if request.input:
            last_in = request.input[-1]
            user_input = getattr(last_in, "content", "")

        # Wrap execution in a root span formatted exactly how MLflow's Chat UI expects.
        with mlflow.start_span(name="MedAtlas Agent", span_type=SpanType.CHAT_MODEL) as root_span:
            root_span.set_inputs({"messages": [{"role": "user", "content": user_input}]})

            tracker = _ToolCallTracker()
            events = self._run_graph(request, tracker)
            outputs = [e.item for e in events if e.type == "response.output_item.done"]

            # Extract final markdown response for the MLflow UI
            last_message = ""
            for out in reversed(outputs):
                if out.type == "message" and out.role == "assistant":
                    if getattr(out, "content", None) and isinstance(out.content, list):
                        last_message = getattr(out.content[0], "text", "")
                    break

            root_span.set_outputs({
                "choices": [{"message": {"role": "assistant", "content": last_message}}]
            })

        return _AgentResult(
            response=ResponsesAgentResponse(output=outputs),
            citations=tracker.get_citations(),
        )

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        """Streaming predict — yields events as they arrive (no citations in stream)."""
        tracker = _ToolCallTracker()
        yield from self._run_graph(request, tracker)


# ─── Export ────────────────────────────────────────────────────────────────────

AGENT = MedAtlasAgent()
mlflow.models.set_model(AGENT)