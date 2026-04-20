# E-Commerce ETL Pipeline

An end-to-end batch data pipeline that ingests e-commerce data from REST APIs, stages it in AWS S3, transforms it with pandas, and loads a star schema data warehouse in Snowflake — orchestrated daily with Apache Airflow.

## Architecture

```
REST APIs          Bronze (S3)           Silver (S3)         Gold (Snowflake)
───────────        ───────────           ───────────         ────────────────
Fake Store API  →  raw/YYYY/MM/DD/    →  staged/YYYY/MM/DD/  →  dim_product
REST Countries     *.json                *.parquet               dim_customer
Open FX Rates                                                    dim_date
                                                                 dim_currency
                                                                 fact_orders
                                                                    ↓
                                                              Reporting Views
                                                              (Power BI)
```

**Pipeline stages:**

1. **Extract** — 5 parallel tasks pull from 3 APIs and upload raw JSON to S3 (bronze layer)
2. **Transform** — pandas cleans, normalizes, and joins data; outputs Parquet to S3 (silver layer)
3. **Load** — Snowflake `COPY INTO` bulk-loads Parquet from S3 via external stage
4. **SQL Models** — 6 reporting views are refreshed in Snowflake for BI consumption

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8.1 |
| Transformation | Python 3.11, pandas, pyarrow |
| Storage | AWS S3 |
| Data Warehouse | Snowflake |
| Containerization | Docker Compose |
| Data Sources | Fake Store API, REST Countries API, Open Exchange Rates API |

## Project Structure

```
ecommerce-etl-pipeline/
├── dags/
│   ├── daily_etl_pipeline.py   # Airflow DAG — schedule, task graph
│   └── utils/
│       ├── api_client.py        # HTTP client wrapper
│       ├── s3_client.py         # S3 upload/download helpers
│       └── snowflake_client.py  # Snowflake execute / COPY INTO
├── etl/
│   ├── extract/extractors.py   # API extractors (FakeStore, Countries, FX)
│   ├── transform/transformers.py # Dimension + fact builders
│   └── load/loaders.py          # Snowflake loader (truncate-and-reload)
├── sql/
│   ├── ddl/create_tables.sql   # Snowflake schema bootstrap
│   └── transforms/aggregations.sql # Reporting views
├── docker-compose.yml
└── requirements.txt
```

## Data Model

Star schema with one fact table and four dimensions:

- **fact_orders** — one row per order line item; clustered by `(DATE_ID, PRODUCT_ID)`
- **dim_product** — product catalog with category, price, and rating
- **dim_customer** — customer profile with geolocation
- **dim_date** — calendar attributes (year, quarter, month, weekday, is_weekend)
- **dim_currency** — USD-base FX rates for currency conversion

## Reporting Views

| View | Description |
|---|---|
| `RPT_DAILY_REVENUE` | Daily revenue, order count, and avg order value |
| `RPT_REVENUE_BY_CATEGORY` | Revenue and units sold by product category |
| `RPT_TOP_PRODUCTS` | Top 10 products ranked by revenue |
| `RPT_ORDERS_BY_CITY` | Order volume and revenue by customer city |
| `RPT_CUSTOMER_LTV` | Lifetime value, order history, and tenure per customer |
| `RPT_WEEKLY_REVENUE` | Weekly revenue with rolling 4-week window |

## Setup

### Prerequisites

- Docker and Docker Compose
- AWS S3 bucket with an IAM user that has `s3:PutObject` / `s3:GetObject` permissions
- Snowflake account
- [Open Exchange Rates](https://openexchangerates.org/) free API key

### 1. Configure environment variables

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

```env
# AWS
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=...
S3_BUCKET=your-bucket-name

# Snowflake
SNOWFLAKE_ACCOUNT=org-account
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_DATABASE=...
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_ROLE=...

# APIs
OPEN_EXCHANGE_RATES_APP_ID=...
```

### 2. Bootstrap the Snowflake schema

Run [sql/ddl/create_tables.sql](sql/ddl/create_tables.sql) in your Snowflake worksheet to create the database, external stage, file format, and tables.

### 3. Start Airflow

```bash
docker-compose up -d
```

Airflow UI: [http://localhost:8080](http://localhost:8080) (admin / admin)

### 4. Enable the DAG

In the Airflow UI, unpause `daily_etl_pipeline`. It runs daily at 06:00 UTC.

## DAG Task Graph

```
extract_products ──┐
extract_carts    ──┤
extract_users    ──┼──► transform ──► load_dimensions ──► load_facts ──► run_sql_models
extract_countries──┤
extract_fx_rates ──┘
```
