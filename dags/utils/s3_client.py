"""
S3 helper — upload/download raw JSON and staged Parquet files.
S3 layout:
  raw/   YYYY/MM/DD/<source>.json    ← bronze layer (immutable landing)
  staged/YYYY/MM/DD/<source>.parquet ← silver layer (cleaned, typed)
"""
import json
import logging
import os
from datetime import date
from io import BytesIO

import boto3
import pandas as pd

logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self, bucket: str | None = None):
        self.bucket = bucket or os.environ["S3_BUCKET"]
        self._s3 = boto3.client("s3")

    # ── Write ────────────────────────────────────────────────────────────────

    def upload_json(self, data: dict | list, source: str, run_date: date | None = None) -> str:
        """Upload raw API response as JSON. Returns the S3 key."""
        d = run_date or date.today()
        key = f"raw/{d.year}/{d.month:02d}/{d.day:02d}/{source}.json"
        body = json.dumps(data, default=str).encode("utf-8")
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json")
        logger.info("Uploaded s3://%s/%s (%d bytes)", self.bucket, key, len(body))
        return key

    def upload_parquet(self, df: pd.DataFrame, source: str, run_date: date | None = None) -> str:
        """Upload transformed DataFrame as Parquet. Returns the S3 key."""
        d = run_date or date.today()
        key = f"staged/{d.year}/{d.month:02d}/{d.day:02d}/{source}.parquet"
        buf = BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=buf.read(), ContentType="application/octet-stream")
        logger.info("Uploaded s3://%s/%s (%d rows)", self.bucket, key, len(df))
        return key

    # ── Read ─────────────────────────────────────────────────────────────────

    def download_json(self, source: str, run_date: date | None = None) -> dict | list:
        d = run_date or date.today()
        key = f"raw/{d.year}/{d.month:02d}/{d.day:02d}/{source}.json"
        obj = self._s3.get_object(Bucket=self.bucket, Key=key)
        return json.loads(obj["Body"].read())

    def download_parquet(self, source: str, run_date: date | None = None) -> pd.DataFrame:
        d = run_date or date.today()
        key = f"staged/{d.year}/{d.month:02d}/{d.day:02d}/{source}.parquet"
        obj = self._s3.get_object(Bucket=self.bucket, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()), engine="pyarrow")

    def s3_uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"
