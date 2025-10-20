# ingestion/understat/client_async.py
from __future__ import annotations
import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

import aiohttp
from understat import Understat

# PitchSights league slug -> Understat league key
LEAGUE_KEY = {
    "Premier-League": "epl",
    "La-Liga":        "la_liga",
    "Serie-A":        "serie_a",
    "Bundesliga":     "bundesliga",
    "Ligue-1":        "ligue_1",
    "RFPL":           "rfpl",
}

def season_label(start_year: int) -> str:
    return f"{start_year}-{start_year+1}"

async def make_client() -> Tuple[aiohttp.ClientSession, Understat]:
    session = aiohttp.ClientSession(
        headers={
            "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/124.0.0.0 Safari/537.36"),
            "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session, Understat(session)

async def close_client(session: aiohttp.ClientSession) -> None:
    try:
        await session.close()
    except Exception:
        pass

# ---------- League fixtures ----------
async def get_league_fixtures(us: Understat, league_slug: str, season_start_year: int) -> List[Dict[str, Any]]:
    key = LEAGUE_KEY.get(league_slug)
    if not key:
        raise ValueError(f"Unsupported league_slug '{league_slug}'. Add it to LEAGUE_KEY.")
    # Understat returns list of match dicts
    return await us.get_league_results(key, season_start_year)

def coerce_fixture_row(m: Dict[str, Any], season: str, league_slug: str) -> Dict[str, Any]:
    mid = str(m.get("id") or m.get("match_id") or "")
    dt = m.get("datetime") or m.get("date") or ""
    match_date, start_time = None, None
    if dt:
        try:
            dttm = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            match_date = dttm.date().isoformat()
            start_time = dttm.time().strftime("%H:%M:%S")
        except Exception:
            parts = dt.split(" ")
            match_date = parts[0] if parts else None
            start_time = parts[1] if len(parts) > 1 else None

    h, a = (m.get("h") or {}), (m.get("a") or {})
    goals = m.get("goals") or {}
    xg    = m.get("xG") or {}

    return {
        "match_id":   mid,
        "match_date": match_date,
        "start_time": start_time,
        "home_team":  (h.get("title") or h.get("short_title") or "").strip(),
        "away_team":  (a.get("title") or a.get("short_title") or "").strip(),
        "home_xG":    (xg.get("h") if isinstance(xg, dict) else None),
        "away_xG":    (xg.get("a") if isinstance(xg, dict) else None),
        "home_score": (goals.get("h") if isinstance(goals, dict) else None),
        "away_score": (goals.get("a") if isinstance(goals, dict) else None),
        "match_url":  f"https://understat.com/match/{mid}" if mid else None,
        "season":     season,
        "league_slug": league_slug,
    }

# ---------- Match shots ----------
async def get_match_shots(us: Understat, match_id: str) -> Dict[str, Any]:
    # Returns dict {'h': [shots...], 'a': [shots...]}
    return await us.get_match_shots(match_id)
