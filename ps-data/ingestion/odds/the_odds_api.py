# ingestion/odds/the_odds_api.py
import os
import json
import time
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ingestion.utils.team_names_mapping import normalize_team

import requests
import pandas as pd
from dateutil import parser as dtparse
from dotenv import load_dotenv

load_dotenv()

# ---------- Config & paths ----------
from ingestion.utils.paths import data_dir

DATA_DIR = data_dir()
RAW_DIR = DATA_DIR / "raw" / "odds"
RAW_DIR.mkdir(parents=True, exist_ok=True)

CACHE_DIR = DATA_DIR / ".cache" / "odds"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

API_BASE = "https://api.the-odds-api.com/v4"
ODDS_API_KEY = "ODDS_API_KEY"

# Default headers
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "PitchSights/odds-ingestor (requests)",
}

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def to_iso8601_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def to_timestamp() -> str:
    return utc_now().strftime("%Y%m%d%H%M%S")

# ---------- Simple file cache ----------
def _cache_key(path: str, params: Dict[str, Any]) -> Path:
    raw = json.dumps([path, sorted(params.items())], separators=(",", ":"), ensure_ascii=False)
    key = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{key}.json"

def _read_cache(path: Path, ttl_sec: int) -> Optional[Any]:
    if not path.exists():
        return None
    age = time.time() - path.stat().st_mtime
    if age <= ttl_sec:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None

def _read_cache_stale(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def _write_cache(path: Path, data: Any) -> None:
    try:
        path.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass

# ---------- Client ----------
class OddsClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        min_interval: float = 1.0,
        default_ttl: int = 15 * 60,
        allow_stale_on_error: bool = True,
    ):
        self.api_key = api_key or os.getenv(ODDS_API_KEY)
        if not self.api_key:
            raise RuntimeError(f"{ODDS_API_KEY} is not set. Add it to your environment or .env.")
        self.min_interval = min_interval
        self.default_ttl = default_ttl
        self.allow_stale_on_error = allow_stale_on_error
        self._session = requests.Session()
        self._last_request_ts = 0.0

    def _rate_limit(self):
        now = time.time()
        delta = now - self._last_request_ts
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)

    def _get(self, path: str, params: Dict[str, Any], ttl_sec: Optional[int]) -> Any:
        params = {**params, "apiKey": self.api_key}
        cache_ttl = self.default_ttl if ttl_sec is None else ttl_sec
        cpath = _cache_key(path, params)

        cached = _read_cache(cpath, cache_ttl)
        if cached is not None:
            return cached

        self._rate_limit()
        try:
            resp = self._session.get(API_BASE + path, headers=HEADERS, params=params, timeout=45)
            self._last_request_ts = time.time()
        except requests.RequestException as e:
            logging.error(f"Network error {path}: {e}")
            if self.allow_stale_on_error:
                stale = _read_cache_stale(cpath)
                if stale is not None:
                    logging.warning("Using stale cache due to network error.")
                    return stale
            raise

        if resp.status_code == 200:
            data = resp.json()
            _write_cache(cpath, data)
            return data

        if resp.status_code in (429, 500, 502, 503, 504, 403):
            ra = resp.headers.get("Retry-After")
            try:
                wait = float(ra)
            except Exception:
                wait = 5.0
            logging.warning(f"{path} -> HTTP {resp.status_code}. Sleep {wait}s; using stale if allowed.")
            time.sleep(wait)
            if self.allow_stale_on_error:
                stale = _read_cache_stale(cpath)
                if stale is not None:
                    logging.warning("Using stale cache after server error.")
                    return stale

        logging.error(f"Non-200 for {path}: {resp.status_code} {resp.text[:250]}")
        resp.raise_for_status()

    def get_odds(
        self,
        sport_key: str,
        regions: str,
        markets: List[str],
        bookmakers: Optional[str] = None,
        commence_from: Optional[datetime] = None,
        commence_to: Optional[datetime] = None,
        ttl_sec: Optional[int] = None,
        odds_format: str = "american",
        date_format: str = "iso",
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "regions": regions,
            "markets": ",".join(markets),
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        }
        if bookmakers:
            params["bookmakers"] = bookmakers
        if commence_from:
            params["commenceTimeFrom"] = to_iso8601_z(commence_from)
        if commence_to:
            params["commenceTimeTo"] = to_iso8601_z(commence_to)

        path = f"/sports/{sport_key}/odds"
        return self._get(path, params, ttl_sec=ttl_sec)  # type: ignore[return-value]

# ---------- Flatten & normalize ----------
def _flatten_events(
    events: List[Dict[str, Any]],
    league_slug: Optional[str] = None,
    snapshot_type: Optional[str] = None,
    anchor_ts: Optional[str] = None,  # for historical “as-of”
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    fetched_at = to_iso8601_z(utc_now())

    for e in events or []:
        event_id = e.get("id")
        sport_key = e.get("sport_key")
        commence_time = e.get("commence_time")
        home = normalize_team(e.get("home_team"), league_slug)
        away = normalize_team(e.get("away_team"), league_slug)

        for bk in e.get("bookmakers", []) or []:
            bkey = bk.get("key")
            blast = bk.get("last_update")
            for m in bk.get("markets", []) or []:
                mkey = m.get("key")
                outcomes = m.get("outcomes", []) or []
                for o in outcomes:
                    name = o.get("name")     # team name / "Over" / "Under" / player name for props
                    price = o.get("price")
                    point = o.get("point")

                    rows.append({
                        "event_id": event_id,
                        "sport_key": sport_key,
                        "commence_time": commence_time,
                        "home_team": home,
                        "away_team": away,
                        "bookmaker": bkey,
                        "market": mkey,
                        "outcome": name,
                        "outcome_desc": None,  # reserve for richer props
                        "point": point,
                        "price": price,
                        "last_update": blast,
                        "snapshot_type": snapshot_type,
                        "fetched_at": fetched_at,
                        "league_slug": league_slug,
                        "anchor_ts": anchor_ts,
                    })
    return pd.DataFrame.from_records(rows)

def _filter_relative_window(df: pd.DataFrame, min_hours: float, max_hours: float, as_of: Optional[datetime]) -> pd.DataFrame:
    if df.empty:
        return df
    ref = as_of or utc_now()
    ct = pd.to_datetime(df["commence_time"], errors="coerce", utc=True)
    hrs = (ct - ref).dt.total_seconds() / 3600.0
    return df.loc[(hrs >= min_hours) & (hrs <= max_hours)].copy()

def _dedup_odds(
    df: pd.DataFrame,
    mode: str = "earliest",
    per_bookmaker: bool = True,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    order_col = "last_update" if "last_update" in df.columns else "fetched_at"
    ascending = (mode == "earliest")
    df2 = df.sort_values(order_col, ascending=ascending)

    key_cols = ["event_id", "market", "outcome"]
    df2["point_key"] = df2["point"].astype(str)
    key_cols.append("point_key")
    if per_bookmaker:
        key_cols.append("bookmaker")

    return df2.drop_duplicates(subset=key_cols, keep="first").drop(columns=["point_key"])

# ---------- Snapshot runner (now supports historical “as_of”) ----------
def run_odds_snapshot(
    snapshot_type: str,
    sport_key: str = "soccer_epl",
    league_slug: str = "Premier-League",
    regions: str = "uk,eu,us",
    bookmakers: Optional[str] = None,
    markets: Optional[List[str]] = None,
    cache_ttl: int = 15 * 60,
    window_min_hours: float = 0,
    window_max_hours: float = 9999,
    season: Optional[str] = None,
    dedup_mode: str = "earliest",
    outfile: Optional[str] = None,          # <-- NEW
) -> Path:
    markets = markets or ["h2h", "spreads", "totals"]

    client = OddsClient(default_ttl=cache_ttl)
    now = utc_now()
    commence_from = now
    commence_to = now + timedelta(hours=window_max_hours + 12)

    logging.info(
        f"Fetching odds snapshot '{snapshot_type}' [{window_min_hours}..{window_max_hours}h], "
        f"sport={sport_key}, regions={regions}, markets={','.join(markets)}, bookmakers={bookmakers or 'ALL'}"
    )

    raw = client.get_odds(
        sport_key=sport_key,
        regions=regions,
        markets=markets,
        bookmakers=bookmakers,
        commence_from=commence_from,
        commence_to=commence_to,
        ttl_sec=cache_ttl,
    )

    df = _flatten_events(raw, league_slug=league_slug, snapshot_type=snapshot_type)

    # Always include an in-file timestamp
    write_ts = to_iso8601_z(utc_now())
    if not df.empty:
        df["snapshot_written_at"] = write_ts
        df["season"] = season
    else:
        # ensure empty file still carries header with timestamp/season
        df = pd.DataFrame([{"snapshot_written_at": write_ts, "season": season}]).iloc[0:0]

    # ----- filename logic -----
    if outfile:
        out = Path(outfile)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        logging.info(f"✅ Saved {len(df)} rows → {out}")
        return out

    # default (timestamped) path for realtime runs
    ts = to_timestamp()
    out = RAW_DIR / f"{snapshot_type}_{league_slug}_{ts}.csv"
    df.to_csv(out, index=False)

    # maintain a simple pointer file only for timestamped writes
    (RAW_DIR / f"{snapshot_type}_{league_slug}_latest.csv").write_text(out.name)

    logging.info(f"✅ Saved {len(df)} rows → {out}")
    return out
