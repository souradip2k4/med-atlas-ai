"""
embedding.py — Generate embeddings for facility facts using the
Databricks Foundation Model API.

Uses batched requests (batch size 32) for efficiency.
"""

import os
import logging
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_DEFAULT_BATCH_SIZE = 32


class EmbeddingGenerator:
    """
    Generate embeddings for fact texts using a Databricks serving endpoint.

    Uses the Foundation Model API ``/serving-endpoints/{endpoint}/invocations``
    with batched inputs.
    """

    def __init__(
        self,
        endpoint: str | None = None,
        batch_size: int = _DEFAULT_BATCH_SIZE,
    ):
        self.host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
        self.token = os.getenv("DATABRICKS_TOKEN", "")
        self.endpoint = endpoint or os.getenv("EMBEDDING_ENDPOINT", "databricks-gte-large-en")
        self.batch_size = batch_size
        self._url = f"{self.host}/serving-endpoints/{self.endpoint}/invocations"
        self._headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    # ── Public API ───────────────────────────────────────────────────

    def generate_embeddings(
        self,
        fact_records: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Generate embeddings for a list of fact records.

        Parameters
        ----------
        fact_records : list[dict]
            Each dict must have at least ``fact_text``.
            Also copies ``fact_id``, ``facility_id``, ``fact_type``,
            ``source_row_id`` into the output.

        Returns
        -------
        list[dict]
            Records matching ``FACILITY_EMBEDDINGS_SCHEMA``.
        """
        results: List[Dict[str, Any]] = []

        # Process in batches
        for batch_start in range(0, len(fact_records), self.batch_size):
            batch = fact_records[batch_start : batch_start + self.batch_size]
            texts = [rec["fact_text"] for rec in batch]

            embeddings = self._call_embedding_api(texts)

            if embeddings is None or len(embeddings) != len(batch):
                logger.error(
                    "Embedding batch %d–%d failed or returned wrong count — skipping",
                    batch_start,
                    batch_start + len(batch),
                )
                continue

            for rec, emb in zip(batch, embeddings):
                results.append({
                    "fact_id": rec["fact_id"],
                    "facility_id": rec["facility_id"],
                    "fact_text": rec["fact_text"],
                    "fact_type": rec.get("fact_type", ""),
                    "embedding": emb,
                    "source_row_id": rec.get("source_row_id", ""),
                })

        logger.info(
            "Generated %d embeddings from %d facts", len(results), len(fact_records)
        )
        return results

    # ── Internal ─────────────────────────────────────────────────────

    def _call_embedding_api(self, texts: List[str]) -> List[List[float]] | None:
        """Call the Databricks embedding endpoint for a batch of texts."""
        payload = {"input": texts}
        try:
            resp = requests.post(
                self._url,
                headers=self._headers,
                json=payload,
                timeout=120,
            )
            resp.raise_for_status()
            data = resp.json()

            # OpenAI-compatible response format
            if "data" in data:
                # Sort by index to ensure order
                sorted_data = sorted(data["data"], key=lambda x: x["index"])
                return [item["embedding"] for item in sorted_data]

            logger.error("Unexpected response format: %s", list(data.keys()))
            return None

        except requests.RequestException as e:
            logger.error("Embedding API request failed: %s", e)
            return None
