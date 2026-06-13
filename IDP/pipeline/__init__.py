# pipeline package
#
# Imports are intentionally lazy (inside functions/access time) to avoid
# importing pyspark / langchain at module load time, which would fail
# in local environments without those packages.

import typing

if typing.TYPE_CHECKING:
    from IDP.pipeline.loader import load_csv_to_delta
    from IDP.pipeline.preprocessor import synthesize_row_text
    from IDP.pipeline.extractor import LLMExtractor
    from IDP.pipeline.merger import merge_extraction_results
    from IDP.pipeline.facility_fact_generator import generate_facts
__all__ = [
    "load_csv_to_delta",
    "synthesize_row_text",
    "LLMExtractor",
    "merge_extraction_results",
    "generate_facts",
]


def __getattr__(name):
    """Lazy import to avoid pyspark/langchain import at module level."""
    if name == "load_csv_to_delta":
        from IDP.pipeline.loader import load_csv_to_delta
        print("Loaded pipeline.loader")
        return load_csv_to_delta
    if name == "synthesize_row_text":
        from IDP.pipeline.preprocessor import synthesize_row_text
        print("Loaded pipeline.preprocessor")
        return synthesize_row_text
    if name == "LLMExtractor":
        from IDP.pipeline.extractor import LLMExtractor
        print("Loaded pipeline.extractor")
        return LLMExtractor
    if name == "merge_extraction_results":
        from IDP.pipeline.merger import merge_extraction_results
        print("Loaded pipeline.merger")
        return merge_extraction_results
    if name == "generate_facts":
        from IDP.pipeline.facility_fact_generator import generate_facts
        print("Loaded pipeline.facility_fact_generator")
        return generate_facts
    raise AttributeError(f"module 'pipeline' has no attribute {name!r}")
