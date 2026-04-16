-- ============================================================
-- Snowflake Reporting Views — consumed by Power BI
-- Run after fact_orders and dimensions are loaded.
-- ============================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA PUBLIC;

-- ── 1. Daily Revenue ──────────────────────────────────────────────────────────
-- KPI trend line: total revenue and order volume per day.
CREATE OR REPLACE VIEW RPT_DAILY_REVENUE AS
SELECT
    d.FULL_DATE,
    d.YEAR,
    d.MONTH_NAME,
    d.WEEKDAY_NAME,
    d.IS_WEEKEND,
    COUNT(DISTINCT f.ORDER_ID)   AS total_orders,
    SUM(f.QUANTITY)              AS units_sold,
    ROUND(SUM(f.TOTAL_USD), 2)   AS revenue_usd,
    ROUND(AVG(f.TOTAL_USD), 2)   AS avg_order_value_usd
FROM FACT_ORDERS f
JOIN DIM_DATE    d ON f.DATE_ID = d.DATE_ID
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1;

-- ── 2. Revenue by Product Category ───────────────────────────────────────────
-- Bar chart: which categories drive the most revenue?
CREATE OR REPLACE VIEW RPT_REVENUE_BY_CATEGORY AS
SELECT
    p.CATEGORY,
    COUNT(DISTINCT f.ORDER_ID)   AS total_orders,
    SUM(f.QUANTITY)              AS units_sold,
    ROUND(SUM(f.TOTAL_USD), 2)   AS revenue_usd,
    ROUND(AVG(p.PRICE_USD), 2)   AS avg_product_price_usd,
    ROUND(AVG(p.RATING_RATE), 2) AS avg_rating
FROM FACT_ORDERS f
JOIN DIM_PRODUCT p ON f.PRODUCT_ID = p.PRODUCT_ID
GROUP BY 1
ORDER BY revenue_usd DESC;

-- ── 3. Top 10 Products by Revenue ────────────────────────────────────────────
-- Table visual: bestsellers with revenue and units.
CREATE OR REPLACE VIEW RPT_TOP_PRODUCTS AS
SELECT
    p.PRODUCT_ID,
    p.NAME                                  AS product_name,
    p.CATEGORY,
    p.PRICE_USD,
    SUM(f.QUANTITY)                          AS units_sold,
    ROUND(SUM(f.TOTAL_USD), 2)               AS revenue_usd,
    RANK() OVER (ORDER BY SUM(f.TOTAL_USD) DESC) AS revenue_rank
FROM FACT_ORDERS f
JOIN DIM_PRODUCT p ON f.PRODUCT_ID = p.PRODUCT_ID
GROUP BY 1, 2, 3, 4
QUALIFY revenue_rank <= 10
ORDER BY revenue_rank;

-- ── 4. Orders by City ─────────────────────────────────────────────────────────
-- Filled map: which cities have the most orders?
CREATE OR REPLACE VIEW RPT_ORDERS_BY_CITY AS
SELECT
    c.CITY,
    c.LAT,
    c.LON,
    COUNT(DISTINCT f.ORDER_ID)   AS total_orders,
    SUM(f.QUANTITY)              AS units_sold,
    ROUND(SUM(f.TOTAL_USD), 2)   AS revenue_usd
FROM FACT_ORDERS f
JOIN DIM_CUSTOMER c ON f.CUSTOMER_ID = c.CUSTOMER_ID
GROUP BY 1, 2, 3
ORDER BY revenue_usd DESC;

-- ── 5. Customer Cohort Summary ────────────────────────────────────────────────
-- How many orders and revenue per customer (lifetime)?
CREATE OR REPLACE VIEW RPT_CUSTOMER_LTV AS
SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME || ' ' || c.LAST_NAME  AS full_name,
    c.EMAIL,
    c.CITY,
    COUNT(DISTINCT f.ORDER_ID)           AS total_orders,
    SUM(f.QUANTITY)                      AS total_units,
    ROUND(SUM(f.TOTAL_USD), 2)           AS lifetime_value_usd,
    MIN(d.FULL_DATE)                     AS first_order_date,
    MAX(d.FULL_DATE)                     AS last_order_date,
    DATEDIFF('day', MIN(d.FULL_DATE), MAX(d.FULL_DATE)) AS customer_tenure_days
FROM FACT_ORDERS  f
JOIN DIM_CUSTOMER c ON f.CUSTOMER_ID = c.CUSTOMER_ID
JOIN DIM_DATE     d ON f.DATE_ID     = d.DATE_ID
GROUP BY 1, 2, 3, 4
ORDER BY lifetime_value_usd DESC;

-- ── 6. Weekly Revenue Trend (rolling 4 weeks) ─────────────────────────────────
-- Useful for Power BI line/area chart with slicer.
CREATE OR REPLACE VIEW RPT_WEEKLY_REVENUE AS
SELECT
    d.YEAR,
    WEEKOFYEAR(d.FULL_DATE)              AS week_num,
    MIN(d.FULL_DATE)                     AS week_start,
    COUNT(DISTINCT f.ORDER_ID)           AS total_orders,
    ROUND(SUM(f.TOTAL_USD), 2)           AS revenue_usd,
    ROUND(
        SUM(SUM(f.TOTAL_USD)) OVER (
            ORDER BY d.YEAR, WEEKOFYEAR(d.FULL_DATE)
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ), 2
    )                                    AS rolling_4wk_revenue_usd
FROM FACT_ORDERS f
JOIN DIM_DATE    d ON f.DATE_ID = d.DATE_ID
GROUP BY 1, 2
ORDER BY 1, 2;
