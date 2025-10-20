import time
import logging
import requests

logging.basicConfig(level=logging.INFO)
HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_url_with_backoff(url, max_attempts=5, initial_delay=2):
    delay = initial_delay
    for attempt in range(1, max_attempts+1):
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code == 200:
            return resp
        elif resp.status_code == 429:
            logging.warning(f"429 Too Many Requests: sleeping {delay}s")
            time.sleep(delay)
            delay *= 2
        else:
            logging.error(f"HTTP {resp.status_code} for {url}")
            break
    return None
# ingestion/utils/http_client.py
"""
HTTP client with polite retries, optional on-disk caching, and basic metrics.

- fetch_url_with_backoff(url, ..., cache_ttl=None, allow_stale_on_error=True)
  -> returns a 'Response-like' object (requests.Response or a CachedResponse)
"""

import hashlib
import logging
import time
import random
from types import SimpleNamespace
from typing import Optional, Dict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from pathlib import Path

import requests

LOGGER = logging.getLogger(__name__)

# ---------- User-Agent rotation & base headers ----------
UA_POOL = [
    # A few recent-ish desktop + mobile UAs (rotate per attempt)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Mobile Safari/537.36",
]

BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "DNT": "1",
}

RETRIABLE_STATUS = {429, 500, 502, 503, 504}
# Treat 403 as possibly transient on some hosts behind anti-bot (we'll backoff more aggressively)
TRANSIENT_403_HOSTS = {"fbref.com"}

# ---------- Simple cache ----------
CACHE_DIR = Path("data/.cache/http")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(url: str) -> Path:
    key = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{key}.cache"


def _read_cache(url: str, ttl_sec: int) -> Optional[str]:
    p = _cache_path(url)
    if not p.exists():
        return None
    age = time.time() - p.stat().st_mtime
    if age <= ttl_sec:
        try:
            return p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return None
    return None


def _read_any_cache(url: str) -> Optional[str]:
    p = _cache_path(url)
    if p.exists():
        try:
            return p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return None
    return None


def _write_cache(url: str, text: str) -> None:
    try:
        _cache_path(url).write_text(text, encoding="utf-8")
    except Exception as e:
        LOGGER.warning("Failed to write cache: %s", e)


def _parse_retry_after(val: Optional[str]) -> float:
    if not val:
        return 0.0
    try:
        return max(float(val), 0.0)
    except ValueError:
        try:
            dt = parsedate_to_datetime(val)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max((dt - datetime.now(timezone.utc)).total_seconds(), 0.0)
        except Exception:
            return 0.0


def _headers_for(url: str, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    headers = dict(BASE_HEADERS)
    headers["User-Agent"] = random.choice(UA_POOL)
    netloc = urlparse(url).netloc
    if "fbref.com" in netloc:
        headers["Referer"] = "https://fbref.com/en/comps/"
    if extra:
        headers.update(extra)
    return headers


# ---------- Metrics ----------
_metrics = {
    "network_requests": 0,
    "cache_hits": 0,
    "by_host": {},  # e.g. {"fbref.com": {"net": X, "cache": Y}, ...}
}


def _bump(host: str, kind: str) -> None:
    if kind == "net":
        _metrics["network_requests"] += 1
    elif kind == "cache":
        _metrics["cache_hits"] += 1
    bucket = _metrics["by_host"].setdefault(host, {"net": 0, "cache": 0})
    bucket[kind] += 1


def get_metrics() -> Dict:
    return _metrics


def reset_metrics() -> None:
    _metrics["network_requests"] = 0
    _metrics["cache_hits"] = 0
    _metrics["by_host"] = {}


def save_metrics(path: str) -> None:
    import json
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(_metrics, indent=2))


# ---------- Response-like wrapper for stale cache ----------
class CachedResponse(SimpleNamespace):
    """Minimal Response-like wrapper when returning cached text."""
    def __init__(self, url: str, text: str, status_code: int = 200, from_cache: bool = True, stale: bool = False):
        super().__init__(url=url, text=text, status_code=status_code, headers={"X-Cache": "HIT" if from_cache else "MISS"})
        self.from_cache = from_cache
        self.stale = stale


def fetch_url_with_backoff(
    url: str,
    max_attempts: int = 8,
    initial_delay: float = 5.0,
    min_interval: float = 1.0,
    timeout: int = 30,
    cache_ttl: Optional[int] = None,          # if provided, use cache before network
    allow_stale_on_error: bool = True,        # if fetch fails, return stale cache if present
    extra_headers: Optional[Dict[str, str]] = None,
) -> Optional[requests.Response]:
    """
    Polite fetch with retries, optional TTL cache, and stale-on-error fallback.

    Returns:
      - requests.Response on network success
      - CachedResponse on cache hit (fresh) or stale fallback
      - None if nothing available
    """
    host = urlparse(url).netloc or "unknown"

    # Fresh cache first (if enabled)
    if cache_ttl and cache_ttl > 0:
        fresh = _read_cache(url, ttl_sec=cache_ttl)
        if fresh is not None:
            _bump(host, "cache")
            return CachedResponse(url, fresh, status_code=200, from_cache=True, stale=False)

    session = requests.Session()
    last_ts = 0.0
    delay = initial_delay

    for attempt in range(1, max_attempts + 1):
        # space out per-process requests a bit
        since = time.time() - last_ts
        if since < min_interval:
            time.sleep(min_interval - since)

        headers = _headers_for(url, extra_headers)
        try:
            resp = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            last_ts = time.time()
            _bump(host, "net")

            # Success
            if resp.status_code == 200:
                if cache_ttl and cache_ttl > 0:
                    _write_cache(url, resp.text)
                return resp

            # Retry/Backoff cases
            if resp.status_code in RETRIABLE_STATUS or (
                resp.status_code == 403 and host in TRANSIENT_403_HOSTS
            ):
                # honor Retry-After if present
                ra = _parse_retry_after(resp.headers.get("Retry-After"))
                wait = max(delay, ra, 30.0 if resp.status_code == 403 else delay)
                LOGGER.warning("[%s/%s] HTTP %s for %s. Sleeping %.1fs",
                               attempt, max_attempts, resp.status_code, url, wait)
                time.sleep(wait + random.uniform(0, 1.5))
                delay = min(delay * 1.8, 120.0)
                continue

            # Non-retriable
            LOGGER.error("Non-retriable HTTP %s for %s. Body: %s", resp.status_code, url, resp.text[:200])
            break

        except requests.RequestException as e:
            LOGGER.warning("[%s/%s] Network error for %s: %s. Sleeping %.1fs",
                           attempt, max_attempts, url, e, delay)
            time.sleep(delay + random.uniform(0, 1.5))
            delay = min(delay * 1.8, 120.0)

    # Fallback to stale cache if allowed
    if allow_stale_on_error:
        stale = _read_any_cache(url)
        if stale is not None:
            LOGGER.warning("Returning STALE cache for %s due to repeated failures.", url)
            _bump(host, "cache")
            return CachedResponse(url, stale, status_code=200, from_cache=True, stale=True)

    LOGGER.error("Failed to fetch %s after %d attempts.", url, max_attempts)
    return None
