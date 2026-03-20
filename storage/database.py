"""
Databricks database manager — session creation and Delta table I/O
"""

import os
import logging

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from databricks.connect import DatabricksSession

load_dotenv()

logger = logging.getLogger(__name__)


class DatabricksDatabase:
    """Manages a Databricks Spark session and Delta table operations."""

    def __init__(self):
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.catalog = os.getenv("CATALOG", "main")
        self.schema = os.getenv("SCHEMA", "default")
        self._spark = None

    # ── Session ──────────────────────────────────────────────────────────

    @property
    def spark(self) -> SparkSession:
        # """Lazy-init Spark session via Databricks Connect."""
        if self._spark is None:
            try:
                

                builder = (
                    DatabricksSession.builder
                    .host(self.host)
                    .token(self.token)
                )

                # Must specify either cluster_id or serverless
                cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
                serverless = os.getenv("DATABRICKS_SERVERLESS", "").lower() in (
                    "true", "1", "yes",
                )

                if cluster_id:
                    builder = builder.clusterId(cluster_id)
                elif serverless:
                    builder = builder.serverless(True)
                else:
                    raise ValueError(
                        "Set DATABRICKS_CLUSTER_ID or DATABRICKS_SERVERLESS=true in .env"
                    )

                self._spark = builder.getOrCreate()

            except ImportError:
                logger.warning(
                    "databricks-connect not installed — falling back to local SparkSession"
                )
                self._spark = (
                    SparkSession.builder
                    .appName("MedAtlasIDP")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config(
                        "spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    )
                    .getOrCreate()
                )

            # Set default catalog + schema
            self._spark.sql(f"USE CATALOG {self.catalog}")
            self._spark.sql(f"USE SCHEMA {self.schema}")

        return self._spark

    # ── Fully-qualified table name ────────────────────────────────────

    def fqn(self, table_name: str) -> str:
        """Return `catalog.schema.table`."""
        return f"{self.catalog}.{self.schema}.{table_name}"

    # ── DDL helpers ──────────────────────────────────────────────────────

    def create_table_if_not_exists(
        self,
        table_name: str,
        schema: StructType,
    ) -> None:
        """
        Register a schema for later use.
        Tables are created lazily on first write via write_delta().
        """
        # We no longer pre-create empty tables—write_delta handles it.
        logger.info("Schema registered for %s", self.fqn(table_name))

    def _table_exists(self, table_name: str) -> bool:
        """Check whether a table exists in the current catalog+schema."""
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.fqn(table_name)}")
            return True
        except Exception:
            return False

    # ── Read / Write ─────────────────────────────────────────────────────

    def write_delta(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
    ) -> None:
        """Write a DataFrame to a Delta table."""
        fqn = self.fqn(table_name)
        is_new_table = not self._table_exists(table_name)
        
        if is_new_table:
            logger.info("Table '%s' does not exist. Initiating creation...", fqn)
            
        try:
            (
                df.write
                .format("delta")
                .mode(mode)
                .option("overwriteSchema", "true")
                .saveAsTable(fqn)
            )
            
            if is_new_table:
                logger.info("Successfully created table '%s' (mode=%s)", fqn, mode)
            else:
                logger.info("Wrote to existing table '%s' (mode=%s)", fqn, mode)
                
        except Exception as e:
            if is_new_table:
                logger.error("Databricks error while creating table '%s': %s", fqn, e)
            else:
                logger.error("Databricks error while writing to table '%s': %s", fqn, e)
            # Re-raise so the pipeline halts/handles it upstream
            raise e

    def append_delta(self, df: DataFrame, table_name: str) -> None:
        """Append a DataFrame to an existing Delta table."""
        self.write_delta(df, table_name, mode="append")

    def read_delta(self, table_name: str) -> DataFrame:
        """Read a Delta table as a DataFrame."""
        fqn = self.fqn(table_name)
        return self.spark.read.format("delta").table(fqn)

    def execute_sql(self, query: str) -> DataFrame:
        """Execute arbitrary SQL and return the result DataFrame."""
        return self.spark.sql(query)
