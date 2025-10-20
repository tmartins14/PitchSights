# ingestion/utils/team_names_mapping.py
"""
Centralized team-name normalization across sources.

- TEAM_NAME_MAPPING keyed by league slug, then source name -> canonical name.
- normalize_team_name(league_slug, name) returns canonical when known, else a tidy fallback.
- map_team_name(league_slug, name) kept for backwards compatibility.
"""

from typing import Dict, Optional

TEAM_NAME_MAPPING: Dict[str, Dict[str, str]] = {
    "Premier-League": {
        "Manchester United":        "Manchester Utd",
        "Newcastle United":         "Newcastle Utd",
        "Nottingham Forest":        "Nott'ham Forest",
        "West Ham United":          "West Ham",
        "Brighton and Hove Albion": "Brighton",
        "Tottenham Hotspur":        "Tottenham",
        "Wolverhampton Wanderers":  "Wolves",
        "Arsenal":                  "Arsenal",
        "Aston Villa":              "Aston Villa",
        "Bournemouth":              "Bournemouth",
        "Brentford":                "Brentford",
        "Chelsea":                  "Chelsea",
        "Crystal Palace":           "Crystal Palace",
        "Everton":                  "Everton",
        "Fulham":                   "Fulham",
        "Ipswich Town":             "Ipswich Town",
        "Leicester City":           "Leicester City",
        "Liverpool":                "Liverpool",
        "Manchester City":          "Manchester City",
        "Southampton":              "Southampton",
        # Common variants
        "Man United":               "Manchester Utd",
        "Man Utd":                  "Manchester Utd",
        "Spurs":                    "Tottenham",
        "Wolves":                   "Wolves",
        "Nottm Forest":             "Nott'ham Forest",
        "West Ham":                 "West Ham",
        "Brighton & Hove Albion":   "Brighton",
        "Man City":                 "Manchester City",
    },
    # Add other leagues here as needed...
}

# ---------- helpers ----------

def _clean(s: str) -> str:
    """Lowercase, strip, normalize separators to improve matching."""
    return (
        (s or "")
        .strip()
        .lower()
        .replace("&", " and ")
        .replace(".", "")
        .replace("-", " ")
        .replace("’", "'")
        .replace("`", "'")
    )

# Build tolerant lookups on first use
_CANON_LOOKUPS: Dict[str, Dict[str, str]] = {}

def _build_lookup_for_league(league_slug: str) -> Dict[str, str]:
    base = TEAM_NAME_MAPPING.get(league_slug, {})
    lookup: Dict[str, str] = {}

    # For each alias -> canonical, index multiple cleaned keys
    for alias, canonical in base.items():
        keys = {
            alias,
            alias.strip(),
            alias.title(),  # sometimes feeds title-case everything
        }
        for k in list(keys):
            keys.add(_clean(k))
        for k in keys:
            lookup[_clean(k)] = canonical

    # Also map canonical name to itself (so canonical in → canonical out)
    for canonical in set(base.values()):
        lookup[_clean(canonical)] = canonical

    return lookup

def _get_lookup(league_slug: Optional[str]) -> Dict[str, str]:
    if not league_slug:
        league_slug = ""  # global bucket
    if league_slug not in _CANON_LOOKUPS:
        _CANON_LOOKUPS[league_slug] = _build_lookup_for_league(league_slug)
    return _CANON_LOOKUPS[league_slug]

# ---------- public API ----------

def normalize_team_name(name: str, league_slug: Optional[str] = None) -> str:
    """
    Return a canonical team name for the given league.
    If not found, return a tidy fallback (title-case with some fixes).
    """
    if not name:
        return ""
    lk = _get_lookup(league_slug)
    hit = lk.get(_clean(name))
    if hit:
        return hit

    # Fallback: reasonable title-casing and small fixes
    parts = [w.capitalize() for w in _clean(name).split()]
    fallback = " ".join(parts)
    # Respect some common forms
    fallback = fallback.replace("Utd", "United")
    fallback = fallback.replace("And", "and")
    return fallback

# Backwards-compatible alias used by other modules
def normalize_team(name: str, league_slug: Optional[str] = None) -> str:
    return normalize_team_name(name, league_slug)

# Backwards compatibility for existing code that imports map_team_name
def map_team_name(league_slug: str, name: str) -> str:
    return normalize_team_name(name, league_slug)

__all__ = [
    "TEAM_NAME_MAPPING",
    "normalize_team_name",
    "normalize_team",          # alias
    "map_team_name",           # legacy alias
]
