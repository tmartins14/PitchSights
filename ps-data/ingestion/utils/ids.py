# ingestion/utils/ids.py
"""
ID / key helpers for FBRef (and similar sources).
"""

from typing import Optional
from urllib.parse import urlparse
import re


def match_id_from_url(url: Optional[str]) -> Optional[str]:
    """
    Extract FBRef match id from URL, e.g.:
      https://fbref.com/en/matches/9c2e11f8/...
    -> "9c2e11f8"
    """
    if not url or not isinstance(url, str):
        return None
    try:
        path = urlparse(url).path or ""
    except Exception:
        return None

    m = re.search(r"/matches/([A-Za-z0-9]+)/?", path)
    if m:
        return m.group(1)

    # Rare fallbacks
    m = re.search(r"/match(?:report)?/([A-Za-z0-9]+)/?", path)
    if m:
        return m.group(1)

    return None
