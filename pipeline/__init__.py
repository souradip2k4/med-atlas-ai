# pipeline package
#
# Imports are intentionally lazy (inside functions/access time) to avoid
# importing pyspark / langchain at module load time, which would fail
# in local environments without those packages.

__all__ = [
    "load_csv_to_delta",
    "synthesize_row_text",
    "LLMExtractor",
    "merge_extraction_results",
    "generate_facts",
    "EmbeddingGenerator",
]


def __getattr__(name):
    """Lazy import to avoid pyspark/langchain import at module level."""
    if name == "load_csv_to_delta":
        from pipeline.loader import load_csv_to_delta
        return load_csv_to_delta
    if name == "synthesize_row_text":
        from pipeline.preprocessor import synthesize_row_text
        return synthesize_row_text
    if name == "LLMExtractor":
        from pipeline.extractor import LLMExtractor
        return LLMExtractor
    if name == "merge_extraction_results":
        from pipeline.merger import merge_extraction_results
        return merge_extraction_results
    if name == "generate_facts":
        from pipeline.fact_generator import generate_facts
        return generate_facts
    if name == "EmbeddingGenerator":
        from pipeline.embedding import EmbeddingGenerator
        return EmbeddingGenerator
    raise AttributeError(f"module 'pipeline' has no attribute {name!r}")
