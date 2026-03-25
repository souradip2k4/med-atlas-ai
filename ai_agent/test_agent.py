"""
Local test harness for MedAtlasAgent.
Run with:  uv run python agent/test_agent.py
"""
import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Load .env
_dot = Path(__file__).parent.parent / ".env"
load_dotenv(_dot)

from .agent import AGENT, MedAtlasAgent, ALL_TOOLS
from mlflow.types.responses import ResponsesRequest, ChatContext

def make_request(question: str) -> ResponsesRequest:
    return ResponsesRequest(
        input=[{"role": "user", "content": question}],
        context=ChatContext(user_id="test@example.com"),
    )

def run(question: str, verbose: bool = True):
    if verbose:
        print(f"\n{'='*70}")
        print(f"Q: {question}")
        print('-'*70)

    req = make_request(question)
    resp = AGENT.predict(req)

    parts = []
    for out in resp.output:
        if getattr(out, "type", "") == "text" and hasattr(out, "content"):
            parts.append(out.content)
    
    if parts:
        answer = "\n".join(parts)
    else:
        answer = str(resp.output)

    if verbose:
        print(f"A: {answer}")
    return answer

if __name__ == "__main__":
    print("Med-Atlas-AI Agent — Local Test Suite")
    print("="*70)

    # ── Query set: one per category ────────────────────────────────────────

    tests = [
        # Genie (quantitative)
        # ("Genie-1", "How many hospitals are in the Ashanti Region?"),
        # ("Genie-2", "What is the total number of beds across all facilities?"),
        # ("Genie-3", "Show the top 5 districts by number of health facilities."),

        # Vector Search (semantic)
        ("VS-1", "Which facilities provide cardiac surgery?"),
        ("VS-2", "What equipment does the main regional hospital have?"),
        ("VS-3", "Find facilities that have MRI machines."),

        # Medical Agent (anomaly detection)
        # ("Med-1", "Detect anomalies in facility records — contradictory signals."),
        # ("Med-2", "Score the reliability of all hospitals."),
        # ("Med-3", "Classify NGO involvement for all facilities."),
        # ("Med-4", "Find over-claiming in facility service claims."),
        # ("Med-5", "Check for equipment-procedure mismatches."),
        # ("Med-6", "Identify unmet needs — service gaps by region."),
        # ("Med-7", "Find duplicate facilities."),

        # # Multi-tool
        # ("Multi-1", "Count health centres in each region AND find anomalies."),
    ]

    results = {}
    for name, question in tests:
        try:
            answer = run(question, verbose=True)
            results[name] = "OK"
        except Exception as exc:
            results[name] = f"FAIL: {exc}"
            print(f"\n{'='*70}")
            print(f"Q: {question}")
            print('-'*70)
            print(f"  {name}: FAIL — {exc}")

    print(f"\n{'='*70}")
    print("Summary:")
    for name, status in results.items():
        icon = "✓" if status == "OK" else "✗"
        print(f"  {icon} {name}: {status}")
