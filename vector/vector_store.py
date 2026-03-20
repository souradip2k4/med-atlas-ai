"""
vector_store.py — Databricks Vector Search index management.

Creates a STANDARD endpoint and a Delta Sync index with
**self-managed (precomputed) embeddings** from the
``facility_embeddings`` table.  NO managed/auto embeddings.
"""

import os
import time
import logging
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class VectorStore:
    """
    Manage a Databricks Vector Search endpoint + index backed by
    precomputed embeddings in ``facility_embeddings``.
    """

    def __init__(
        self,
        endpoint_name: str | None = None,
        source_table: str | None = None,
    ):
        from databricks.vector_search.client import VectorSearchClient

        self.host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
        self.token = os.getenv("DATABRICKS_TOKEN", "")
        self.catalog = os.getenv("CATALOG", "main")
        self.schema = os.getenv("SCHEMA", "default")

        self.endpoint_name = endpoint_name or os.getenv(
            "VS_ENDPOINT_NAME", "med-atlas-vs-endpoint"
        )
        self.source_table = source_table or (
            f"{self.catalog}.{self.schema}.facility_embeddings"
        )
        self.index_name = f"{self.catalog}.{self.schema}.facility_embeddings_index"

        self.vsc = VectorSearchClient(
            workspace_url=self.host,
            personal_access_token=self.token,
        )

    # ── Endpoint management ──────────────────────────────────────────

    def create_endpoint(self) -> None:
        """Create the Vector Search endpoint if it does not exist."""
        try:
            self.vsc.get_endpoint(self.endpoint_name)
            logger.info("Endpoint '%s' already exists", self.endpoint_name)
        except Exception:
            logger.info("Creating endpoint '%s' ...", self.endpoint_name)
            self.vsc.create_endpoint(
                name=self.endpoint_name,
                endpoint_type="STANDARD",
            )
            self._wait_for_endpoint()

    def _wait_for_endpoint(self, timeout: int = 600, poll: int = 15) -> None:
        """Poll until the endpoint is ONLINE."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                ep = self.vsc.get_endpoint(self.endpoint_name)
                status = ep.get("endpoint_status", {}).get("state", "")
                if status == "ONLINE":
                    logger.info("Endpoint '%s' is ONLINE", self.endpoint_name)
                    return
                logger.info("Endpoint status: %s — waiting...", status)
            except Exception as e:
                logger.debug("Polling endpoint: %s", e)
            time.sleep(poll)
        raise TimeoutError(
            f"Endpoint '{self.endpoint_name}' did not come online in {timeout}s"
        )

    # ── Index management ─────────────────────────────────────────────

    def create_index(self) -> None:
        """
        Create a Delta Sync index with **self-managed** embeddings.

        The index reads the ``embedding`` column directly from the
        ``facility_embeddings`` table — no managed embedding generation.
        """
        try:
            self.vsc.get_index(
                endpoint_name=self.endpoint_name,
                index_name=self.index_name,
            )
            logger.info("Index '%s' already exists", self.index_name)
            return
        except Exception:
            pass

        logger.info(
            "Creating Delta Sync index '%s' on table '%s' ...",
            self.index_name,
            self.source_table,
        )
        self.vsc.create_delta_sync_index(
            endpoint_name=self.endpoint_name,
            index_name=self.index_name,
            source_table_name=self.source_table,
            primary_key="fact_id",
            pipeline_type="TRIGGERED",
            embedding_dimension=1024,  # databricks-gte-large-en dimension
            embedding_vector_column="embedding",
        )
        logger.info("Index creation initiated. Use sync_index() to trigger sync.")

    def sync_index(self) -> None:
        """Trigger a sync of the Delta Sync index."""
        idx = self.vsc.get_index(
            endpoint_name=self.endpoint_name,
            index_name=self.index_name,
        )
        idx.sync()
        logger.info("Sync triggered for index '%s'", self.index_name)

    # ── Query ────────────────────────────────────────────────────────

    def query(
        self,
        query_text: str | None = None,
        query_vector: List[float] | None = None,
        num_results: int = 10,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Similarity search against the vector index.

        Supply either ``query_text`` (for managed-embedding indexes)
        or ``query_vector`` (for self-managed indexes).
        """
        if columns is None:
            columns = ["fact_id", "facility_id", "fact_text", "fact_type"]

        idx = self.vsc.get_index(
            endpoint_name=self.endpoint_name,
            index_name=self.index_name,
        )

        kwargs: Dict[str, Any] = {
            "columns": columns,
            "num_results": num_results,
        }
        if query_vector is not None:
            kwargs["query_vector"] = query_vector
        elif query_text is not None:
            kwargs["query_text"] = query_text
        if filters:
            kwargs["filters"] = filters

        results = idx.similarity_search(**kwargs)

        rows: List[Dict[str, Any]] = []
        if results and "result" in results:
            col_names = results["result"].get("column_names", columns + ["score"])
            for data_row in results["result"].get("data_array", []):
                rows.append(dict(zip(col_names, data_row)))

        return rows
