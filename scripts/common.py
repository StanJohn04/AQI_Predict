"""Shared configuration, logging, HTTP and database plumbing for the AQI pipeline.

Everything that used to be copy-pasted across etl.py / historical_backfill.py /
historical_patch.py lives here exactly once.
"""
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

LOG_DIR = REPO_ROOT / "logs"

# Google's history endpoint reaches back exactly 30 days and refuses any window
# ending less than ~2 hours ago. Both were measured against the live API on
# 2026-08-19; see Docs/code-review-2026-08-04.md.
GOOGLE_HISTORY_MAX_AGE_DAYS = 30
GOOGLE_HISTORY_MIN_LAG_HOURS = 2

HTTP_TIMEOUT = 30
HTTP_RETRIES = 3

LOCATIONS = [
    {"city": "Savannah",       "country": "USA", "latitude": 32.0809, "longitude": -81.0912,
     "timezone": "America/New_York"},
    {"city": "Port Wentworth", "country": "USA", "latitude": 32.17,   "longitude": -81.17,
     "timezone": "America/New_York"},
    {"city": "Pooler",         "country": "USA", "latitude": 32.11,   "longitude": -81.25,
     "timezone": "America/New_York"},
]


class FetchError(Exception):
    """A network or API failure that the caller must not silently swallow."""


def setup_logging(name: str) -> logging.Logger:
    """Log to both stdout and logs/<name>-YYYY-MM-DD.log."""
    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)

    fileh = logging.FileHandler(LOG_DIR / f"{name}-{date.today().isoformat()}.log",
                                encoding="utf-8")
    fileh.setFormatter(fmt)
    logger.addHandler(fileh)
    return logger


def get_engine() -> Engine:
    """Build the engine and prove the connection works. Raises on any failure."""
    missing = [k for k in ("DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME")
               if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required .env variables: {', '.join(missing)}")

    # URL.create quotes the password properly, so special characters can never
    # corrupt the connection string the way the old f-string allowed.
    url = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_NAME"),
    )
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine


def request_json(method: str, url: str, **kwargs) -> dict:
    """HTTP with a timeout, bounded retries, and the response body on error.

    The old code printed only the exception, which for a 4xx omits the body --
    exactly where Google puts the real reason: key expired, quota exhausted,
    billing disabled, API not enabled (F2).
    """
    kwargs.setdefault("timeout", HTTP_TIMEOUT)
    last = ""
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            resp = requests.request(method, url, **kwargs)
            if resp.status_code == 200:
                return resp.json()
            last = f"HTTP {resp.status_code}: {resp.text[:500]}"
            # 4xx other than rate-limiting will not improve on retry.
            if 400 <= resp.status_code < 500 and resp.status_code != 429:
                raise FetchError(f"{url} -> {last}")
        except requests.exceptions.RequestException as exc:
            last = f"request error: {exc}"
        if attempt < HTTP_RETRIES:
            time.sleep(2 * attempt)
    raise FetchError(f"{url} failed after {HTTP_RETRIES} attempts -> {last}")
