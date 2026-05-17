"""
=============================================================
  EXTRACTORS / API_EXTRACTOR.PY  —  The Web Data Fetcher
=============================================================

WHAT IS AN API?
  API = Application Programming Interface.

  Imagine a restaurant menu.
  You (the customer) can't walk into the kitchen and grab food.
  Instead you tell the waiter (the API) what you want, and the
  waiter brings it back on a plate (the response).

  APIs let us ask websites for data in a structured way.
  The data comes back as JSON — a text format that looks like
  a Python dictionary.

  Example API request:
    GET https://api.example.com/sales?year=2024
  Example response:
    { "sales": [{"id": 1, "amount": 500}, {"id": 2, "amount": 300}] }

PAGINATION — WHAT IS IT?
  Most APIs don't give you ALL data at once — they give you
  "pages" like a book.  Page 1 has rows 1-100, page 2 has
  rows 101-200, etc.  We keep asking for the next page until
  there are no more.
=============================================================
"""

import time
from typing import Iterator, Optional
import requests
import pandas as pd
from loguru import logger
from .base_extractor import BaseExtractor


class APIExtractor(BaseExtractor):
    """
    Fetches data from REST APIs with automatic pagination,
    retry logic, and rate-limit handling.
    """

    def __init__(
        self,
        base_url: str,
        endpoint: str,
        api_key: Optional[str] = None,
        batch_size: int = 100,
        max_pages: int = 100,
        rate_limit_delay: float = 0.2,
        params: Optional[dict] = None,
    ):
        """
        base_url        : the root URL, e.g. "https://api.example.com"
        endpoint        : the specific route, e.g. "/v2/sales"
        api_key         : secret password for the API (kept out of logs)
        batch_size      : rows per page request
        max_pages       : safety limit — don't fetch more than this many pages
        rate_limit_delay: seconds to wait between requests (be a polite guest)
        params          : extra query parameters, e.g. {"country": "US"}
        """
        super().__init__(source_name=f"API:{base_url}{endpoint}", batch_size=batch_size)
        self.base_url = base_url.rstrip("/")
        self.endpoint = endpoint
        self.api_key = api_key
        self.max_pages = max_pages
        self.rate_limit_delay = rate_limit_delay
        self.extra_params = params or {}
        self._session = None   # reusable HTTP connection (faster than new one each time)

    # ── CONNECT ───────────────────────────────────────────────
    def connect(self) -> bool:
        """
        Create an HTTP session and test the connection with a
        tiny request.
        """
        self._session = requests.Session()

        # Set auth header if we have an API key
        if self.api_key:
            self._session.headers.update({"Authorization": f"Bearer {self.api_key}"})

        # Always ask for JSON back
        self._session.headers.update({"Accept": "application/json"})

        # Quick health check — try fetching just 1 item
        try:
            test_url = f"{self.base_url}{self.endpoint}"
            resp = self._session.get(test_url, params={"limit": 1}, timeout=10)
            resp.raise_for_status()   # raises an error if HTTP status is 4xx/5xx
            self._connected = True
            logger.info(f"API connection successful: {test_url}")
            return True
        except requests.RequestException as e:
            logger.warning(f"API health check failed: {e} — will still attempt extraction")
            self._connected = True   # allow attempt anyway
            return True

    # ── EXTRACT ───────────────────────────────────────────────
    def extract(self) -> Iterator[pd.DataFrame]:
        """
        Fetch all pages of data, one batch at a time.
        """
        if not self._connected:
            raise RuntimeError("Call connect() before extract()")

        url = f"{self.base_url}{self.endpoint}"
        page = 1

        while page <= self.max_pages:
            params = {
                "page": page,
                "limit": self.batch_size,
                **self.extra_params,    # merge in any extra params
            }

            # Retry logic — try up to 3 times before giving up
            for attempt in range(1, 4):
                try:
                    response = self._session.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    break   # success — exit the retry loop
                except requests.HTTPError as e:
                    if response.status_code == 429:
                        # 429 = "Too Many Requests" — back off and wait
                        wait = int(response.headers.get("Retry-After", 60))
                        logger.warning(f"Rate limited. Waiting {wait}s...")
                        time.sleep(wait)
                    elif attempt == 3:
                        logger.error(f"API request failed after 3 attempts: {e}")
                        return
                    else:
                        time.sleep(2 ** attempt)  # exponential back-off: 2s, 4s, 8s
                except requests.RequestException as e:
                    logger.error(f"Network error on page {page}: {e}")
                    return

            # Parse JSON response
            data = response.json()

            # Handle both list responses and nested {"data": [...]} responses
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                # Common patterns: "data", "results", "items", "records"
                for key in ("data", "results", "items", "records"):
                    if key in data:
                        records = data[key]
                        break
                else:
                    records = [data]   # single-record response
            else:
                logger.warning(f"Unexpected response format on page {page}")
                break

            if not records:
                logger.info(f"No more data at page {page}. Extraction complete.")
                break

            df = pd.DataFrame(records)
            df["_source_page"] = page
            df["_extracted_at"] = pd.Timestamp.now()

            logger.debug(f"API page {page}: {len(df)} rows")
            yield df

            if len(records) < self.batch_size:
                # Got fewer rows than requested — must be the last page
                break

            page += 1
            time.sleep(self.rate_limit_delay)   # be polite: don't hammer the server

    # ── CLOSE ─────────────────────────────────────────────────
    def close(self):
        if self._session:
            self._session.close()
        self._connected = False
        logger.debug("API session closed")
