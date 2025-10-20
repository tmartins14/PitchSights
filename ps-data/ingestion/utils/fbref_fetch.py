# ingestion/utils/fbref_fetch.py
"""
FBRef HTML fetchers backed by http_client and a dedicated sub-cache.
No Playwright here; bs4-only scrapers can rely on these helpers.
"""

from typing import Optional
from pathlib import Path
import hashlib
import time
import logging

from .http_client import fetch_url_with_backoff

LOGGER = logging.getLogger(__name__)

CACHE_DIR = Path("data/.cache/http/fbref")
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


def _write_cache(url: str, html: str) -> None:
    try:
        _cache_path(url).write_text(html, encoding="utf-8")
    except Exception as e:
        LOGGER.warning("FBRef cache write failed: %s", e)


def fetch_fbref_html(url: str, timeout: int = 30) -> Optional[str]:
    """
    Direct fetch via http_client (no local fbref cache).
    """
    resp = fetch_url_with_backoff(url, timeout=timeout)
    if resp and getattr(resp, "status_code", None) == 200:
        return resp.text
    return None


def fetch_fbref_html_cached(url: str, timeout: int = 30, ttl_sec: int = 24 * 3600) -> Optional[str]:
    """
    Use local fbref-only cache first; else network; else stale cache; else None.
    """
    fresh = _read_cache(url, ttl_sec)
    if fresh is not None:
        return fresh

    resp = fetch_url_with_backoff(url, timeout=timeout)
    if resp and getattr(resp, "status_code", None) == 200:
        html = resp.text
        _write_cache(url, html)
        return html

    stale = _read_any_cache(url)
    if stale is not None:
        LOGGER.warning("FBRef: returning STALE cache for %s", url)
        return stale

    return None
