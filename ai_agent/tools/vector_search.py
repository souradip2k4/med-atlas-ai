from ai_agent.config import VS_INDEX
from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit
from databricks_langchain import VectorSearchRetrieverTool
from langchain.tools import tool
import json

# ─── Tool 2 — Vector Search ───────────────────────────────────────────────────

@tool
def vector_search_tool(
    query: str,
    fact_types: list[str] | str | None = None,
    num_results: int = 100,
) -> str:
    """
    Semantic search over pre-generated facility facts stored in the facility_facts table.

    Best for: "Which facilities provide cardiac surgery?", "has MRI?",
    "similar to [name]", specialized services, capabilities, equipment.

    Args:
        query:       Natural language search query
        fact_types:  Optional filter to specific fact types.
                     Valid values and what each type contains:
                       - "specialty"   : Medical specialty tags a facility offers
                                         (e.g., internalMedicine, dentistry, gynecologyAndObstetrics)
                       - "procedure"   : Specific medical procedures performed in plain text
                                         (e.g., "Offers teeth whitening", "fertility management")
                       - "equipment"   : Physical devices/machines on-site
                                         (e.g., "Automatic changeover oxygen manifold", "operating room equipment")
                       - "capability"  : Operational context — hours, departments, contact info,
                                         accreditations, social media, 24/7 availability
                       - "summary"     : One-line facility profile: type, location, affiliation,
                                         general description
                     Pass a list like ["procedure", "equipment"] or a single string like "specialty".
                     If None, searches across all fact types (use only when cross-type context is needed).
        num_results: Number of results to return from the vector index.
                     Default: 100 (sufficient for pure-semantic queries).
                     Use 350 for IS_COLDSPOT (1★) and IS_GEOSPATIAL+IS_SEMANTIC (2★) pipelines
                     to maximize the pool available for geospatial–semantic intersection.
    """
    from databricks_langchain import VectorSearchRetrieverTool

    if isinstance(fact_types, str):
        fact_types = [fact_types]

    kwargs = {
        "index_name": VS_INDEX,
        "num_results": num_results,
        "reranker": {
            "model": "databricks_reranker",
            "parameters": {
                "columns_to_rerank": ["fact_text"]
            }
        },
        # fact_id is the VS index primary key — MUST be included for retrieval to work.
        # It is intentionally excluded from the JSON output returned to the LLM (see below).
        "columns": ["fact_id", "facility_id", "fact_text", "fact_type"],
    }

    if fact_types and len(fact_types) == 1:
        kwargs["filters"] = {"fact_type": fact_types[0]}
    elif fact_types and len(fact_types) > 1:
        # Databricks VS multi-value filter: pass list directly as the value (NOT MongoDB $in syntax)
        kwargs["filters"] = {"fact_type": fact_types}

    post_filter_types = set(fact_types) if fact_types and len(fact_types) > 1 else None

    try:
        vs = VectorSearchRetrieverTool(**kwargs)
        results = vs.invoke({"query": query})
        
        print("\n" + "="*80)
        print("DEBUG: RAW VS INVOKE RESULTS (before JSON structuring):")
        print(results)
        print("="*80 + "\n")
        
        if post_filter_types and isinstance(results, list):
            results = [
                doc for doc in results
                if doc.metadata.get("fact_type") in post_filter_types
            ]
        # Convert Document list to structured JSON — drop fact_id (not needed by LLM)
        if isinstance(results, list):
            structured = [
                {
                    "facility_id": doc.metadata.get("facility_id", ""),
                    "fact_type":   doc.metadata.get("fact_type", ""),
                    "fact_text":   doc.page_content,
                }
                for doc in results
            ]
            return json.dumps({"results": structured, "total_results": len(structured)}, indent=2)
        return json.dumps({"results": [], "total_results": 0})
    except Exception as exc:
        return f"[Vector Search Error] {exc}"