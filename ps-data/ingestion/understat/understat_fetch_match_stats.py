#!/usr/bin/env python3
"""
Basic Understat fetcher for match-level shots/xG data.

✅ Works without a browser (no Selenium/Playwright). Understat embeds JSON in <script> tags.
✅ Pulls JSON, decodes it, and normalizes to rows.

Usage examples:
  # Single match
  python understat_fetch_match_stats.py match --match-id 21519 --out data/understat/matches

  # By league & season (collect all match IDs from the league page, then pull each match)
  python understat_fetch_match_stats.py league --league EPL --season 2024 --out data/understat/matches --max 50

Notes:
- Supported leagues typically include: EPL, La_Liga, Serie_A, Bundesliga, Ligue_1, RFPL
- Be respectful: add delays, set a UA string, and cache if you scale this up.
- Python 3.9.6 compatible.
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from ingestion.utils.paths import data_dir

DATA_DIR = data_dir()
RAW_DIR = DATA_DIR / "raw" / "understat"
INTERIM_DIR = DATA_DIR / "interim" / "understat"
RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)  # only needed in normalize.py


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

UNDERSTAT_BASE = "https://understat.com"

# --- Utilities -----------------------------------------------------------------

class FetchError(RuntimeError):
    pass


def fetch_html(url: str, sleep_s: float = 1.2, retries: int = 3, timeout: int = 20) -> str:
    """Fetch a URL and return HTML text with basic retry & politeness."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            if resp.status_code == 200:
                time.sleep(sleep_s)
                return resp.text
            elif 400 <= resp.status_code < 500:
                raise FetchError(f"Client error {resp.status_code} for {url}")
            else:
                last_exc = FetchError(
                    f"Unexpected status {resp.status_code} for {url} (attempt {attempt}/{retries})"
                )
        except Exception as e:  # noqa: BLE001
            last_exc = e
        time.sleep(sleep_s * attempt)
    raise FetchError(str(last_exc) if last_exc else f"Failed to fetch {url}")


_JSON_PARSE_PATTERNS = [
    # var foo = JSON.parse('...')
    re.compile(r"var\s+(?P<name>\w+)\s*=\s*JSON\.parse\('\s*(?P<payload>[^']*?)\s*'\)\s*;?", re.DOTALL),
    # var foo = JSON.parse(decodeURIComponent('...'))
    re.compile(
        r"var\s+(?P<name>\w+)\s*=\s*JSON\.parse\(\s*decodeURIComponent\('\s*(?P<payload>[^']*?)\s*'\)\s*\)\s*;?",
        re.DOTALL,
    ),
]


def _try_decode_json_payload(payload: str) -> Any:
    """Decode a JSON string found inside JSON.parse('...') or decodeURIComponent('...')."""
    # Understat escapes backslashes and quotes; unicode_escape helps unescape sequences.
    try:
        decoded = payload.encode("utf-8").decode("unicode_escape")
        return json.loads(decoded)
    except Exception:
        # Fallback: try URL-decoding semantics (some pages use encodeURIComponent chains)
        try:
            from urllib.parse import unquote

            decoded = unquote(payload)
            decoded = decoded.encode("utf-8").decode("unicode_escape")
            return json.loads(decoded)
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"Failed to decode embedded JSON payload: {e}")


def extract_embedded_json_vars(html: str) -> Dict[str, Any]:
    """Parse all `var NAME = JSON.parse('...')` blobs into a dict {NAME: value}.

    This is robust to both plain JSON.parse('...') and JSON.parse(decodeURIComponent('...')).
    """
    soup = BeautifulSoup(html, "lxml")
    out: Dict[str, Any] = {}

    for script in soup.find_all("script"):
        txt = script.string or script.text or ""
        if not txt:
            continue
        for pat in _JSON_PARSE_PATTERNS:
            for m in pat.finditer(txt):
                name = m.group("name")
                raw = m.group("payload")
                try:
                    out[name] = _try_decode_json_payload(raw)
                except Exception:
                    # Keep going; some variables may not decode cleanly
                    pass
    return out


# --- League & match helpers ----------------------------------------------------

@dataclass
class MatchShot:
    match_id: int
    side: str  # 'h' or 'a'
    minute: Optional[int]
    X: Optional[float]
    Y: Optional[float]
    xG: Optional[float]
    result: Optional[str]
    player: Optional[str]
    player_id: Optional[int]
    situation: Optional[str]
    shotType: Optional[str]
    assistedBy: Optional[str]
    keyPassId: Optional[int]
    home: Optional[str]
    away: Optional[str]
    season: Optional[str]
    league: Optional[str]


LEAGUE_PATHS = {
    # Common Understat league slugs
    "EPL": "EPL",
    "La_Liga": "La_Liga",
    "Serie_A": "Serie_A",
    "Bundesliga": "Bundesliga",
    "Ligue_1": "Ligue_1",
    "RFPL": "RFPL",
}


def get_league_match_ids(league: str, season: int) -> List[int]:
    """Return all match IDs listed on a league+season page.

    Understat league page typically exposes variables like: datesData, matchesData, teamsData.
    We try to extract from matchesData (list of matches with 'id').
    """
    league_slug = LEAGUE_PATHS.get(league, league)
    url = f"{UNDERSTAT_BASE}/league/{league_slug}/{season}"
    html = fetch_html(url)
    blobs = extract_embedded_json_vars(html)

    # Try multiple candidates that sometimes contain match IDs
    match_ids: List[int] = []

    # 1) matchesData is often a dict of date -> list[matches]
    matches_data = blobs.get("matchesData")
    if isinstance(matches_data, dict):
        for _date, matches in matches_data.items():
            if isinstance(matches, list):
                for m in matches:
                    mid = m.get("id") or m.get("match_id")
                    if mid is not None:
                        try:
                            match_ids.append(int(mid))
                        except Exception:
                            pass

    # 2) Set Fallbacks: some pages keep IDs inside "datesData" entries as well
    if not match_ids:
        dates_data = blobs.get("datesData")
        if isinstance(dates_data, list):
            for entry in dates_data:
                for m in entry.get("matches", []):
                    mid = m.get("id") or m.get("match_id")
                    if mid is not None:
                        try:
                            match_ids.append(int(mid))
                        except Exception:
                            pass

    # Return unique, sorted
    return sorted(set(match_ids))


def get_match_shots(match_id: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Fetch a match page and return (shots_rows, meta).

    Understat match page exposes one or more of:
      - shotsData (dict with 'h' & 'a' lists)
      - shotsDataHome / shotsDataAway
      - match_info / teamsData / etc. (used to enrich output)
    """
    url = f"{UNDERSTAT_BASE}/match/{match_id}"
    html = fetch_html(url)
    blobs = extract_embedded_json_vars(html)

    shots: Dict[str, List[Dict[str, Any]]] = {"h": [], "a": []}

    if isinstance(blobs.get("shotsData"), dict):
        maybe = blobs["shotsData"]
        shots["h"] = maybe.get("h") or maybe.get("home") or []
        shots["a"] = maybe.get("a") or maybe.get("away") or []
    else:
        # Try split vars
        if isinstance(blobs.get("shotsDataHome"), list):
            shots["h"] = blobs["shotsDataHome"]
        if isinstance(blobs.get("shotsDataAway"), list):
            shots["a"] = blobs["shotsDataAway"]

    meta: Dict[str, Any] = {}
    # Best-effort meta enrichment
    for key in ("match_info", "matchInfo", "teamsData", "teamData", "stadiumData", "weatherData"):
        if key in blobs:
            meta[key] = blobs[key]

    return [*shots.get("h", []), *shots.get("a", [])], meta


def normalize_shots(rows: Iterable[Dict[str, Any]], match_id: int, meta: Dict[str, Any]) -> List[MatchShot]:
    # Try to derive home/away team names
    home_name = None
    away_name = None
    if isinstance(meta.get("match_info"), dict):
        home_name = meta["match_info"].get("h", {}).get("title")
        away_name = meta["match_info"].get("a", {}).get("title")
    elif isinstance(meta.get("teamsData"), dict):
        # teamsData is often a dict of {teamId: {title: name, side: 'h'/'a'}}
        for _, t in meta["teamsData"].items():
            if t.get("side") == "h":
                home_name = t.get("title")
            elif t.get("side") == "a":
                away_name = t.get("title")

    out: List[MatchShot] = []
    for r in rows:
        side = r.get("h_team") and "h" or (r.get("a_team") and "a")
        # Understat shots usually have 'X', 'Y', 'xG', 'player', 'player_id', 'result', 'situation', 'shotType', 'minute', 'h_team', 'a_team', 'keyPassId', 'assistedBy'
        out.append(
            MatchShot(
                match_id=match_id,
                side=side or ("h" if r.get("h_a") == "h" else "a" if r.get("h_a") == "a" else None),
                minute=_safe_int(r.get("minute")),
                X=_safe_float(r.get("X")),
                Y=_safe_float(r.get("Y")),
                xG=_safe_float(r.get("xG")),
                result=_safe_str(r.get("result")),
                player=_safe_str(r.get("player")),
                player_id=_safe_int(r.get("player_id")),
                situation=_safe_str(r.get("situation")),
                shotType=_safe_str(r.get("shotType")),
                assistedBy=_safe_str(r.get("assistedBy")),
                keyPassId=_safe_int(r.get("keyPassId")),
                home=home_name or _safe_str(r.get("h_team")),
                away=away_name or _safe_str(r.get("a_team")),
                season=_safe_str(r.get("season")),
                league=_safe_str(r.get("league")),
            )
        )
    return out


def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v) if v is not None and v != "" else None
    except Exception:
        return None


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None and v != "" else None
    except Exception:
        return None


def _safe_str(v: Any) -> Optional[str]:
    try:
        return str(v) if v is not None and v != "" else None
    except Exception:
        return None


# --- I/O -----------------------------------------------------------------------

CSV_FIELDS = [
    "match_id",
    "side",
    "minute",
    "X",
    "Y",
    "xG",
    "result",
    "player",
    "player_id",
    "situation",
    "shotType",
    "assistedBy",
    "keyPassId",
    "home",
    "away",
    "season",
    "league",
]


def write_shots_csv(path: str, rows: List[MatchShot]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r.__dict__)


# --- CLI -----------------------------------------------------------------------

def cmd_match(args: argparse.Namespace) -> None:
    match_id = int(args.match_id)
    shots, meta = get_match_shots(match_id)
    norm = normalize_shots(shots, match_id, meta)

    out_dir = args.out or "data/understat/matches"
    out_path = os.path.join(out_dir, str(match_id)[:4], f"match_{match_id}_shots.csv")
    write_shots_csv(out_path, norm)
    print(f"Saved {len(norm)} shots to {out_path}")


def cmd_league(args: argparse.Namespace) -> None:
    league = args.league
    season = int(args.season)
    out_dir = args.out or "data/understat/matches"
    max_n = args.max

    ids = get_league_match_ids(league, season)
    if max_n:
        ids = ids[: max_n]
    if not ids:
        print("No match IDs found. Check league/season.")
        return

    total_rows = 0
    for i, mid in enumerate(ids, 1):
        try:
            shots, meta = get_match_shots(mid)
            norm = normalize_shots(shots, mid, meta)
            total_rows += len(norm)
            out_path = os.path.join(out_dir, str(season), f"match_{mid}_shots.csv")
            write_shots_csv(out_path, norm)
            print(f"[{i}/{len(ids)}] match {mid}: {len(norm)} shots")
        except Exception as e:  # noqa: BLE001
            print(f"[{i}/{len(ids)}] match {mid}: ERROR {e}")

    print(f"Done. Wrote {len(ids)} matches, {total_rows} total shots.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Understat match shots fetcher")

    sub = p.add_subparsers(dest="cmd", required=True)

    p_match = sub.add_parser("match", help="Fetch a single match by ID")
    p_match.add_argument("--match-id", required=True, help="Understat match ID (int)")
    p_match.add_argument("--out", default=f"{RAW_DIR}/matches", help="Output base directory")
    p_match.set_defaults(func=cmd_match)

    p_league = sub.add_parser("league", help="Fetch all matches from a league season")
    p_league.add_argument("--league", required=True, help="League slug (e.g., EPL, La_Liga, Serie_A)")
    p_league.add_argument("--season", required=True, help="Season year (e.g., 2024)")
    p_league.add_argument("--max", type=int, default=None, help="Max matches to fetch (for testing)")
    p_league.add_argument("--out", default=f"{RAW_DIR}/matches", help="Output base directory")
    p_league.set_defaults(func=cmd_league)

    return p


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
