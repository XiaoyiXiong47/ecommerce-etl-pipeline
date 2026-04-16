"""
Loader — copies staged Parquet files from S3 into Snowflake tables.

Pre-requisites in Snowflake (run sql/ddl/create_tables.sql first):
  - External stage:  ECOMMERCE_STAGE  pointing to s3://<bucket>/staged/
  - File format:     PARQUET_FORMAT
  - Target tables:   dim_product, dim_customer, dim_date, dim_currency, fact_orders
"""
import logging
from datetime import date

from dags.utils.snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)

# Ordered so dimensions load before facts
LOAD_ORDER = ["dim_product", "dim_customer", "dim_date", "dim_currency", "fact_orders"]


class SnowflakeLoader:
    def __init__(self, snowflake: SnowflakeClient, stage: str = "ECOMMERCE_STAGE"):
        self.sf = snowflake
        self.stage = stage

    def load_table(self, table: str, run_date: date) -> list[dict]:
        """
        COPY INTO <table> from the S3 external stage for a given run date.
        Snowflake resolves the prefix: staged/YYYY/MM/DD/<table>.parquet
        """
        d = run_date
        prefix = f"staged/{d.year}/{d.month:02d}/{d.day:02d}/{table}.parquet"
        logger.info("Loading %s from %s", table, prefix)
        return self.sf.copy_into_from_stage(
            table=table.upper(),
            stage=self.stage,
            s3_prefix=prefix,
        )

    def load_all(self, run_date: date | None = None) -> dict[str, list]:
        """Load all tables in dependency order. Returns results per table."""
        d = run_date or date.today()
        results = {}
        for table in LOAD_ORDER:
            results[table] = self.load_table(table, d)
        logger.info("All tables loaded for %s", d)
        return results

    def truncate_and_reload(self, table: str, run_date: date | None = None):
        """Truncate a table before loading — useful for full-refresh dimensions."""
        self.sf.execute(f"TRUNCATE TABLE {table.upper()};")
        self.load_table(table, run_date or date.today())
