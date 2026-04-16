"""
Snowflake helper — execute SQL and run COPY INTO from S3 external stage.
"""
import logging
import os

import snowflake.connector
from snowflake.connector import DictCursor

logger = logging.getLogger(__name__)


def _conn_params() -> dict:
    return {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role": os.environ["SNOWFLAKE_ROLE"],
    }


class SnowflakeClient:
    def __init__(self):
        self._conn = snowflake.connector.connect(**_conn_params())

    def execute(self, sql: str, params: tuple | None = None) -> list[dict]:
        """Run arbitrary SQL and return rows as list of dicts."""
        with self._conn.cursor(DictCursor) as cur:
            logger.info("Executing: %s", sql[:120])
            cur.execute(sql, params)
            return cur.fetchall()

    def execute_many(self, sql: str, rows: list[tuple]) -> int:
        """Bulk insert using executemany. Returns row count."""
        with self._conn.cursor() as cur:
            cur.executemany(sql, rows)
            self._conn.commit()
            return cur.rowcount

    def copy_into_from_stage(
        self,
        table: str,
        stage: str,
        s3_prefix: str,
        file_format: str = "PARQUET_FORMAT",
    ) -> list[dict]:
        """
        COPY INTO <table> from an external stage pointing to S3.
        The external stage and file format must be pre-created in Snowflake.
        """
        sql = f"""
            COPY INTO {table}
            FROM @{stage}/{s3_prefix}
            FILE_FORMAT = (FORMAT_NAME = '{file_format}')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
            PURGE = FALSE;
        """
        results = self.execute(sql)
        logger.info("COPY INTO %s: %s", table, results)
        return results

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
