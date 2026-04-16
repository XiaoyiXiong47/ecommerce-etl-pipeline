"""
daily_etl_pipeline — main Airflow DAG

Schedule: daily at 06:00 UTC (after midnight so APIs reflect prior day).

Task dependency graph:

  extract_products ──┐
  extract_carts    ──┤
  extract_users    ──┤─► transform ──► load_dimensions ──► load_facts ──► run_sql_models
  extract_countries──┤
  extract_fx_rates ──┘
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Airflow mounts ./etl into /opt/airflow/etl inside the container
import sys
sys.path.insert(0, "/opt/airflow")

from dags.utils.s3_client import S3Client
from dags.utils.snowflake_client import SnowflakeClient
from etl.extract.extractors import FakeStoreExtractor, RESTCountriesExtractor, FXRatesExtractor
from etl.transform.transformers import EcommerceTransformer
from etl.load.loaders import SnowflakeLoader

logger = logging.getLogger(__name__)

# ── Default args ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Task functions ────────────────────────────────────────────────────────────

def _run_date(context) -> date:
    """Derive the logical run date from the Airflow execution date."""
    return context["data_interval_start"].date()


def extract_products(**context):
    run_date = _run_date(context)
    s3 = S3Client()
    FakeStoreExtractor(s3).extract_products(run_date)


def extract_carts(**context):
    run_date = _run_date(context)
    s3 = S3Client()
    FakeStoreExtractor(s3).extract_carts(run_date)


def extract_users(**context):
    run_date = _run_date(context)
    s3 = S3Client()
    FakeStoreExtractor(s3).extract_users(run_date)


def extract_countries(**context):
    run_date = _run_date(context)
    s3 = S3Client()
    RESTCountriesExtractor(s3).extract_countries(run_date)


def extract_fx_rates(**context):
    run_date = _run_date(context)
    s3 = S3Client()
    FXRatesExtractor(s3).extract_rates(run_date)


def transform(**context):
    run_date = _run_date(context)
    s3 = S3Client()
    keys = EcommerceTransformer(s3, run_date).run()
    logger.info("Transform complete: %s", keys)


def load_dimensions(**context):
    run_date = _run_date(context)
    with SnowflakeClient() as sf:
        loader = SnowflakeLoader(sf)
        for table in ["dim_product", "dim_customer", "dim_date", "dim_currency"]:
            loader.truncate_and_reload(table, run_date)


def load_facts(**context):
    run_date = _run_date(context)
    with SnowflakeClient() as sf:
        loader = SnowflakeLoader(sf)
        loader.load_table("fact_orders", run_date)


def run_sql_models(**context):
    """Refresh materialized aggregation views in Snowflake."""
    with SnowflakeClient() as sf:
        for view in [
            "RPT_DAILY_REVENUE",
            "RPT_REVENUE_BY_CATEGORY",
            "RPT_TOP_PRODUCTS",
            "RPT_ORDERS_BY_COUNTRY",
        ]:
            sf.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM {view}_DEF;")
    logger.info("SQL models refreshed")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="daily_etl_pipeline",
    description="E-commerce ETL: Fake Store API → S3 → Snowflake",
    schedule="0 6 * * *",           # daily at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "etl", "portfolio"],
) as dag:

    # ── Extract ───────────────────────────────────────────────────────────────
    with TaskGroup("extract") as extract_group:
        t_products = PythonOperator(task_id="extract_products", python_callable=extract_products)
        t_carts    = PythonOperator(task_id="extract_carts",    python_callable=extract_carts)
        t_users    = PythonOperator(task_id="extract_users",    python_callable=extract_users)
        t_countries= PythonOperator(task_id="extract_countries",python_callable=extract_countries)
        t_fx       = PythonOperator(task_id="extract_fx_rates", python_callable=extract_fx_rates)

    # ── Transform ─────────────────────────────────────────────────────────────
    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    # ── Load ──────────────────────────────────────────────────────────────────
    with TaskGroup("load") as load_group:
        t_load_dims  = PythonOperator(task_id="load_dimensions", python_callable=load_dimensions)
        t_load_facts = PythonOperator(task_id="load_facts",      python_callable=load_facts)
        t_load_dims >> t_load_facts   # dimensions before facts

    # ── SQL models ────────────────────────────────────────────────────────────
    t_sql_models = PythonOperator(
        task_id="run_sql_models",
        python_callable=run_sql_models,
    )

    # ── Dependency chain ──────────────────────────────────────────────────────
    extract_group >> t_transform >> load_group >> t_sql_models
