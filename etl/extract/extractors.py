"""
Extractor classes — one per data source.
Each extractor pulls data from its API and uploads the raw JSON to S3.
"""
import logging
import os
from datetime import date

from dags.utils.api_client import APIClient
from dags.utils.s3_client import S3Client

logger = logging.getLogger(__name__)


class FakeStoreExtractor:
    """
    Pulls products, carts (orders), and users from fakestoreapi.com.
    No API key required.
    """

    BASE_URL = "https://fakestoreapi.com"

    def __init__(self, s3: S3Client):
        self.s3 = s3

    def extract_products(self, run_date: date | None = None) -> str:
        with APIClient(self.BASE_URL) as client:
            data = client.get("/products")
        logger.info("Fetched %d products", len(data))
        return self.s3.upload_json(data, "products", run_date)

    def extract_carts(self, run_date: date | None = None) -> str:
        """Carts represent orders — each cart has user_id, date, and products."""
        with APIClient(self.BASE_URL) as client:
            data = client.get("/carts")
        logger.info("Fetched %d carts", len(data))
        return self.s3.upload_json(data, "carts", run_date)

    def extract_users(self, run_date: date | None = None) -> str:
        with APIClient(self.BASE_URL) as client:
            data = client.get("/users")
        logger.info("Fetched %d users", len(data))
        return self.s3.upload_json(data, "users", run_date)


class RESTCountriesExtractor:
    """
    Pulls country metadata from restcountries.com.
    Used to enrich user addresses with region, continent, and currency info.
    No API key required.
    """

    BASE_URL = "https://restcountries.com/v3.1"

    def __init__(self, s3: S3Client):
        self.s3 = s3

    def extract_countries(self, run_date: date | None = None) -> str:
        with APIClient(self.BASE_URL) as client:
            data = client.get("/all", params={"fields": "name,cca2,region,subregion,currencies,latlng,population"})
        logger.info("Fetched %d countries", len(data))
        return self.s3.upload_json(data, "countries", run_date)


class FXRatesExtractor:
    """
    Pulls live USD-base FX rates from openexchangerates.org.
    Requires OPEN_EXCHANGE_RATES_APP_ID env var (free tier: 1,000 req/month).
    """

    BASE_URL = "https://openexchangerates.org/api"

    def __init__(self, s3: S3Client):
        self.s3 = s3
        self.app_id = os.environ["OPEN_EXCHANGE_RATES_APP_ID"]

    def extract_rates(self, run_date: date | None = None) -> str:
        with APIClient(self.BASE_URL) as client:
            data = client.get("/latest.json", params={"app_id": self.app_id, "base": "USD"})
        logger.info("Fetched %d FX rates (base: %s)", len(data.get("rates", {})), data.get("base"))
        return self.s3.upload_json(data, "fx_rates", run_date)
