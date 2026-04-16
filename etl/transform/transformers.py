"""
Transformer classes — clean, normalize, and join raw data.
Reads JSON from S3 (bronze), writes Parquet back to S3 (silver).
"""
import logging
from datetime import date

import pandas as pd

from dags.utils.s3_client import S3Client

logger = logging.getLogger(__name__)


class EcommerceTransformer:
    """
    Builds the following clean DataFrames ready for Snowflake loading:
      - fact_orders
      - dim_product
      - dim_customer
      - dim_date
      - dim_currency
    """

    def __init__(self, s3: S3Client, run_date: date | None = None):
        self.s3 = s3
        self.run_date = run_date or date.today()

    # ── Raw loaders ──────────────────────────────────────────────────────────

    def _load_raw(self) -> tuple[list, list, list, list, dict]:
        products = self.s3.download_json("products", self.run_date)
        carts = self.s3.download_json("carts", self.run_date)
        users = self.s3.download_json("users", self.run_date)
        countries = self.s3.download_json("countries", self.run_date)
        fx_data = self.s3.download_json("fx_rates", self.run_date)
        return products, carts, users, countries, fx_data

    # ── Dimension builders ───────────────────────────────────────────────────

    def build_dim_product(self, products: list) -> pd.DataFrame:
        df = pd.DataFrame(products)
        df = df.rename(columns={"id": "product_id", "title": "name"})
        df["category"] = df["category"].str.strip().str.lower()
        df["price_usd"] = df["price"].astype(float).round(2)
        df["rating_rate"] = df["rating"].apply(lambda r: r.get("rate") if isinstance(r, dict) else None)
        df["rating_count"] = df["rating"].apply(lambda r: r.get("count") if isinstance(r, dict) else None)
        return df[["product_id", "name", "category", "price_usd", "rating_rate", "rating_count"]]

    def build_dim_customer(self, users: list) -> pd.DataFrame:
        rows = []
        for u in users:
            addr = u.get("address", {})
            geo = addr.get("geolocation", {})
            rows.append({
                "customer_id": u["id"],
                "first_name": u.get("name", {}).get("firstname", ""),
                "last_name": u.get("name", {}).get("lastname", ""),
                "email": u.get("email", ""),
                "city": addr.get("city", ""),
                "zipcode": addr.get("zipcode", ""),
                "lat": float(geo.get("lat", 0) or 0),
                "lon": float(geo.get("long", 0) or 0),
            })
        return pd.DataFrame(rows)

    def build_dim_date(self, carts: list) -> pd.DataFrame:
        dates = pd.to_datetime([c["date"] for c in carts if c.get("date")], errors="coerce").dropna()
        unique_dates = pd.Series(dates.normalize().unique()).rename("full_date")
        df = pd.DataFrame({"full_date": unique_dates})
        df["date_id"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
        df["year"] = df["full_date"].dt.year
        df["month"] = df["full_date"].dt.month
        df["month_name"] = df["full_date"].dt.strftime("%B")
        df["day"] = df["full_date"].dt.day
        df["weekday_num"] = df["full_date"].dt.weekday   # 0=Mon
        df["weekday_name"] = df["full_date"].dt.strftime("%A")
        df["is_weekend"] = df["weekday_num"].isin([5, 6])
        df["quarter"] = df["full_date"].dt.quarter
        return df[["date_id", "full_date", "year", "quarter", "month", "month_name", "day", "weekday_num", "weekday_name", "is_weekend"]]

    def build_dim_currency(self, fx_data: dict) -> pd.DataFrame:
        rates = fx_data.get("rates", {})
        rows = [{"currency_code": code, "rate_to_usd": 1 / rate if rate else None}
                for code, rate in rates.items() if rate]
        df = pd.DataFrame(rows)
        df["currency_id"] = range(1, len(df) + 1)
        return df[["currency_id", "currency_code", "rate_to_usd"]]

    def build_fact_orders(
        self,
        carts: list,
        products_df: pd.DataFrame,
        customers_df: pd.DataFrame,
        dates_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Explode carts into one row per line item.
        Join to dim tables to get price_usd and date_id.
        """
        rows = []
        product_prices = products_df.set_index("product_id")["price_usd"].to_dict()

        for cart in carts:
            cart_date = pd.to_datetime(cart.get("date"), errors="coerce")
            if pd.isna(cart_date):
                continue
            date_id = int(cart_date.normalize().strftime("%Y%m%d"))

            for item in cart.get("products", []):
                pid = item.get("productId")
                qty = item.get("quantity", 1)
                unit_price = product_prices.get(pid, 0.0)
                rows.append({
                    "order_id": cart["id"],
                    "date_id": date_id,
                    "product_id": pid,
                    "customer_id": cart.get("userId"),
                    "quantity": qty,
                    "unit_price_usd": unit_price,
                    "total_usd": round(unit_price * qty, 2),
                })

        return pd.DataFrame(rows)

    # ── Orchestrate ──────────────────────────────────────────────────────────

    def run(self) -> dict[str, str]:
        """
        Run all transforms and upload Parquet files to S3.
        Returns dict of {table_name: s3_key}.
        """
        products, carts, users, countries, fx_data = self._load_raw()

        dim_product = self.build_dim_product(products)
        dim_customer = self.build_dim_customer(users)
        dim_date = self.build_dim_date(carts)
        dim_currency = self.build_dim_currency(fx_data)
        fact_orders = self.build_fact_orders(carts, dim_product, dim_customer, dim_date)

        keys = {}
        for name, df in [
            ("dim_product", dim_product),
            ("dim_customer", dim_customer),
            ("dim_date", dim_date),
            ("dim_currency", dim_currency),
            ("fact_orders", fact_orders),
        ]:
            key = self.s3.upload_parquet(df, name, self.run_date)
            keys[name] = key
            logger.info("Transformed %s: %d rows → %s", name, len(df), key)

        return keys
