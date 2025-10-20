# ingestion/utils/__init__.py
"""
Convenience exports for ingestion utilities.
"""

# HTTP / caching / metrics
from .http_client import (
    fetch_url_with_backoff,
    get_metrics,
    reset_metrics,
    save_metrics,
)

# I/O helpers
from .io import (
    write_raw,
    write_interim,
    utc_timestamp,
)

# Parsing helpers
from .parsing import (
    parse_score,
)

# ID helpers
from .ids import (
    match_id_from_url,
)

# FBRef HTML fetchers
from .fbref_fetch import (
    fetch_fbref_html,
    fetch_fbref_html_cached,
)

# Team-name normalization
from .team_names_mapping import (
    TEAM_NAME_MAPPING,
    map_team_name,
)

__all__ = [
    # http_client
    "fetch_url_with_backoff",
    "get_metrics",
    "reset_metrics",
    "save_metrics",
    # io
    "write_raw",
    "write_interim",
    "utc_timestamp",
    # parsing
    "parse_score",
    # ids
    "match_id_from_url",
    # fbref
    "fetch_fbref_html",
    "fetch_fbref_html_cached",
    # teams
    "TEAM_NAME_MAPPING",
    "map_team_name",
]
