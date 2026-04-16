"""
Reusable HTTP API client with retry logic and rate limiting.
"""
import time
import logging
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)


class APIClient:
    """Generic HTTP client with automatic retries and basic rate limiting."""

    def __init__(self, base_url: str, headers: dict | None = None, rate_limit_delay: float = 0.5):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(headers or {})
        self.rate_limit_delay = rate_limit_delay  # seconds between requests

    @retry(
        retry=retry_if_exception_type(requests.HTTPError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def get(self, endpoint: str, params: dict | None = None) -> Any:
        """GET request with automatic retries on 5xx errors."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info("GET %s params=%s", url, params)
        response = self.session.get(url, params=params, timeout=30)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            logger.warning("Rate limited. Sleeping %ds", retry_after)
            time.sleep(retry_after)
            response.raise_for_status()

        response.raise_for_status()
        time.sleep(self.rate_limit_delay)
        return response.json()

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
