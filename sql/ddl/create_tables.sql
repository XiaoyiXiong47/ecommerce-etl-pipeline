-- ============================================================
-- Snowflake DDL — E-commerce Data Warehouse
-- Run once to bootstrap the schema before the first DAG run.
-- ============================================================

-- ── Database & Schema ────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DW;
USE DATABASE ECOMMERCE_DW;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- ── File Format (for COPY INTO from Parquet) ─────────────────────────────────
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE;

-- ── External Stage (points to your S3 bucket) ────────────────────────────────
-- Replace <YOUR_BUCKET> and <YOUR_IAM_ROLE_ARN> with your actual values.
-- Alternatively use AWS key/secret pair:
--   CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...')
CREATE OR REPLACE STAGE ECOMMERCE_STAGE
    URL = 's3://<YOUR_BUCKET>/'
    CREDENTIALS = (AWS_ROLE = '<YOUR_IAM_ROLE_ARN>')
    FILE_FORMAT = PARQUET_FORMAT
    COMMENT = 'ETL landing zone — bronze (raw/) and silver (staged/) layers';

-- ── Dimension: dim_date ───────────────────────────────────────────────────────
CREATE OR REPLACE TABLE DIM_DATE (
    DATE_ID         INT             NOT NULL PRIMARY KEY,  -- YYYYMMDD
    FULL_DATE       DATE            NOT NULL,
    YEAR            SMALLINT        NOT NULL,
    QUARTER         TINYINT         NOT NULL,
    MONTH           TINYINT         NOT NULL,
    MONTH_NAME      VARCHAR(10)     NOT NULL,
    DAY             TINYINT         NOT NULL,
    WEEKDAY_NUM     TINYINT         NOT NULL,              -- 0=Mon, 6=Sun
    WEEKDAY_NAME    VARCHAR(10)     NOT NULL,
    IS_WEEKEND      BOOLEAN         NOT NULL
);

-- ── Dimension: dim_product ────────────────────────────────────────────────────
CREATE OR REPLACE TABLE DIM_PRODUCT (
    PRODUCT_ID      INT             NOT NULL PRIMARY KEY,
    NAME            VARCHAR(500)    NOT NULL,
    CATEGORY        VARCHAR(100)    NOT NULL,
    PRICE_USD       DECIMAL(10, 2)  NOT NULL,
    RATING_RATE     DECIMAL(3, 2),
    RATING_COUNT    INT
);

-- ── Dimension: dim_customer ───────────────────────────────────────────────────
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    CUSTOMER_ID     INT             NOT NULL PRIMARY KEY,
    FIRST_NAME      VARCHAR(100),
    LAST_NAME       VARCHAR(100),
    EMAIL           VARCHAR(255),
    CITY            VARCHAR(100),
    ZIPCODE         VARCHAR(20),
    LAT             DECIMAL(9, 6),
    LON             DECIMAL(9, 6)
);

-- ── Dimension: dim_currency ───────────────────────────────────────────────────
CREATE OR REPLACE TABLE DIM_CURRENCY (
    CURRENCY_ID     INT             NOT NULL PRIMARY KEY,
    CURRENCY_CODE   VARCHAR(3)      NOT NULL UNIQUE,
    RATE_TO_USD     DECIMAL(18, 8)  NOT NULL  -- How many USD = 1 unit of this currency
);

-- ── Fact: fact_orders ─────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE FACT_ORDERS (
    ORDER_LINE_ID   INT             NOT NULL AUTOINCREMENT PRIMARY KEY,
    ORDER_ID        INT             NOT NULL,
    DATE_ID         INT             NOT NULL REFERENCES DIM_DATE(DATE_ID),
    PRODUCT_ID      INT             NOT NULL REFERENCES DIM_PRODUCT(PRODUCT_ID),
    CUSTOMER_ID     INT             NOT NULL REFERENCES DIM_CUSTOMER(CUSTOMER_ID),
    QUANTITY        INT             NOT NULL DEFAULT 1,
    UNIT_PRICE_USD  DECIMAL(10, 2)  NOT NULL,
    TOTAL_USD       DECIMAL(12, 2)  NOT NULL,
    LOADED_AT       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ── Indexes (Snowflake uses clustering keys for large tables) ─────────────────
ALTER TABLE FACT_ORDERS CLUSTER BY (DATE_ID, PRODUCT_ID);

-- ── Verification queries ──────────────────────────────────────────────────────
-- SELECT COUNT(*) FROM DIM_PRODUCT;
-- SELECT COUNT(*) FROM DIM_CUSTOMER;
-- SELECT COUNT(*) FROM FACT_ORDERS;
-- SELECT * FROM FACT_ORDERS LIMIT 5;
