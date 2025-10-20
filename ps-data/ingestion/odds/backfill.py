# ingestion/odds/backfill.py
from __future__ import annotations

import os, json, time, logging, argparse, hashlib
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple, Set

import requests
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

# # ---------------- .env (optional) ----------------
# try:
#     from dotenv import load_dotenv, find_dotenv
#     _env = find_dotenv(usecwd=True)
#     load_dotenv(_env) if _env else load_dotenv()
# except Exception:
#     pass

ODDS_API_KEY = "ODDS_API_KEY"
API_BASE = "https://api.the-odds-api.com/v4"
HEADERS = {"Accept": "application/json", "User-Agent": "PitchSights/season-backfill"}

# Snapshot anchors (hours pre-KO); single anchor per snapshot
SNAPSHOT_ANCHORS = {
    "openers": 132.0,   # ~midpoint of [96..168]h pre-KO
    "midweek": 60.0,    # ~midpoint of [48..72]h pre-KO
    "bet":     24.0,    # ~1 day pre-KO
    "close":   1.0,     # ~1h pre-KO
    # --- New in-play snapshots (negative means after kickoff) ---
    "inplay_20min":  -20.0 / 60.0,   # 20 minutes after KO
    "inplay_halftime": -45.0 / 60.0, # 45 minutes after KO
    "inplay_70min":  -70.0 / 60.0,   # 70 minutes after KO
}

# Unified output schema
EMPTY_COLUMNS = [
    "event_id","sport_key","commence_time","home_team","away_team",
    "bookmaker","market","player","direction","line","price",
    "last_update","snapshot_type","snapshot_at","snapshot_written_at",
    "season","league_slug",
]

# ---------------- time utils ----------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def to_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_iso_utc(s: str) -> Optional[datetime]:
    try:
        return pd.to_datetime(s, utc=True).to_pydatetime()
    except Exception:
        return None

# ---------------- lightweight HTTP cache ----------------
def _cache_key(path: str, params: Dict[str, Any]) -> str:
    # ignore apiKey in cache key
    clean = {k: v for k, v in params.items() if k != "apiKey"}
    raw = json.dumps([path, sorted(clean.items())], separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

class HttpCache:
    def __init__(self, base: Path, ttl_sec: int):
        self.base = base; self.ttl = max(0, int(ttl_sec))
        self.base.mkdir(parents=True, exist_ok=True)
    def get(self, key: str) -> Optional[Any]:
        if self.ttl <= 0: return None
        p = self.base / f"{key}.json"
        if not p.exists(): return None
        if time.time() - p.stat().st_mtime > self.ttl: return None
        try: return json.loads(p.read_text(encoding="utf-8"))
        except Exception: return None
    def put(self, key: str, data: Any) -> None:
        if self.ttl <= 0: return
        p = self.base / f"{key}.json"
        try: p.write_text(json.dumps(data), encoding="utf-8")
        except Exception: pass

# ---------------- API Client ----------------
class Client:
    """
    Uses the *historical* endpoints that worked in your simple script:
      - /historical/sports/{sport_key}/events?date=YYYY-MM-DDThh:mm:ssZ
      - /historical/sports/{sport_key}/events/{event_id}/odds?date=...
    """
    def __init__(self, api_key: Optional[str] = None, min_interval: float = 1.0, timeout: int = 45,
                 cache_ttl: int = 0, log_usage: bool = False):
        self.api_key = api_key or os.getenv(ODDS_API_KEY)
        if not self.api_key:
            raise RuntimeError(f"{ODDS_API_KEY} not set (env or .env)")
        self.s = requests.Session()
        self.min_interval = min_interval
        self.timeout = timeout
        self._last = 0.0
        self.cache = HttpCache(Path("data/.cache/odds_http"), cache_ttl)
        self.log_usage = log_usage

    def _sleep(self):
        gap = time.time() - self._last
        if gap < self.min_interval:
            time.sleep(self.min_interval - gap)

    def _get(self, path: str, params: Dict[str, Any]) -> Any:
        key = _cache_key(path, params)
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        self._sleep()
        pp = dict(params); pp["apiKey"] = self.api_key
        r = self.s.get(API_BASE + path, params=pp, headers=HEADERS, timeout=self.timeout)
        self._last = time.time()
        if self.log_usage:
            rem = r.headers.get("x-requests-remaining") or r.headers.get("X-Requests-Remaining")
            usd = r.headers.get("x-requests-used") or r.headers.get("X-Requests-Used")
            logging.info("GET %s -> %s  remain=%s used=%s", path, r.status_code, rem, usd)
        if r.status_code == 200:
            try:
                data = r.json()
                self.cache.put(key, data)
                return data
            except Exception:
                return None
        logging.error("GET %s %s\n%s", path, r.status_code, (r.text or "")[:400])
        r.raise_for_status()

    # date → events (historical)
    def historical_events_for_iso(self, sport_key: str, iso_anchor: str) -> List[Dict[str, Any]]:
        path = f"/historical/sports/{sport_key}/events"
        data = self._get(path, {"dateFormat": "iso", "date": iso_anchor})
        items = []
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            items = data["data"]
        elif isinstance(data, list):
            items = data
        out: List[Dict[str, Any]] = []
        for e in items or []:
            if "id" in e and "commence_time" in e:
                out.append({
                    "id": e.get("id"),
                    "sport_key": sport_key,
                    "commence_time": e.get("commence_time"),
                    "home_team": e.get("home_team"),
                    "away_team": e.get("away_team"),
                })
        return out

    # event@time → odds (historical)
    def historical_odds_for_event(
        self, sport_key: str, event_id: str, iso_snapshot_time: str,
        regions: str, markets_csv: str, bookmakers: Optional[str],
        odds_format: str = "decimal", date_format: str = "iso",
    ) -> Any:
        path = f"/historical/sports/{sport_key}/events/{event_id}/odds"
        params = {
            "regions": regions,
            "markets": markets_csv,
            "dateFormat": date_format,
            "oddsFormat": odds_format,
            "date": iso_snapshot_time,
        }
        if bookmakers:
            params["bookmakers"] = bookmakers
        return self._get(path, params)

# ---------------- flatten & write ----------------
def _extract_bookmakers(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("bookmakers"), list):
        return payload["bookmakers"]
    d = payload.get("data")
    if isinstance(d, dict) and isinstance(d.get("bookmakers"), list):
        return d["bookmakers"]
    return []

def _flatten_payload(
    payload: Any,
    event: Dict[str, Any],
    snapshot_type: str,
    iso_snapshot_time: str,
    season: str,
    league_slug: str,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    eid = event.get("id")
    sport_key = event.get("sport_key")
    commence_time = event.get("commence_time")
    home = event.get("home_team")
    away = event.get("away_team")
    written_at = to_iso_z(utcnow())

    for bk in _extract_bookmakers(payload):
        bkey = bk.get("key") or bk.get("title")
        blast = bk.get("last_update") or bk.get("updated_at")
        for m in bk.get("markets") or []:
            mkey = m.get("key")
            for o in m.get("outcomes") or []:
                rows.append({
                    "event_id": eid,
                    "sport_key": sport_key,
                    "commence_time": commence_time,
                    "home_team": home,
                    "away_team": away,
                    "bookmaker": bkey,
                    "market": mkey,
                    "player": o.get("description"),
                    "direction": o.get("name"),
                    "line": o.get("point"),
                    "price": o.get("price"),
                    "last_update": blast,
                    "snapshot_type": snapshot_type,
                    "snapshot_at": iso_snapshot_time,
                    "snapshot_written_at": written_at,
                    "season": season,
                    "league_slug": league_slug,
                })
    return pd.DataFrame.from_records(rows, columns=EMPTY_COLUMNS)

def _append_dedup(out_path: Path, df_new: pd.DataFrame) -> None:
    """
    Append immediately and dedupe on-disk:
      key = (event_id, bookmaker, market, player, direction, line, snapshot_at)
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        try:
            prev = pd.read_csv(out_path)
            if not prev.empty and not df_new.empty:
                df_new = pd.concat([prev, df_new], ignore_index=True)
            elif not prev.empty and df_new.empty:
                # nothing to add; keep prev
                prev.to_csv(out_path, index=False)
                return
        except Exception:
            # if prior file is corrupted, just write new
            pass
    if not df_new.empty:
        df_new["line_key"] = df_new["line"].astype(str)
        df_new.drop_duplicates(
            subset=["event_id","bookmaker","market","player","direction","line_key","snapshot_at"],
            keep="last", inplace=True
        )
        df_new.drop(columns=["line_key"], inplace=True)
    df_new.to_csv(out_path, index=False)

# Track what’s already present so we can skip API calls
def _load_completed_index(out_path: Path) -> Dict[Tuple[str, str], Set[str]]:
    """
    Returns {(event_id, snapshot_at) -> set(markets_present)}.
    """
    present: Dict[Tuple[str, str], Set[str]] = {}
    if not out_path.exists():
        return present
    try:
        df = pd.read_csv(out_path, usecols=["event_id","snapshot_at","market"])
        df = df.dropna(subset=["event_id","snapshot_at","market"])
        for r in df.itertuples(index=False):
            key = (str(getattr(r, "event_id")), str(getattr(r, "snapshot_at")))
            present.setdefault(key, set()).add(str(getattr(r, "market")))
    except Exception:
        pass
    return present

# ---------------- main (date → events → odds; write per event) ----------------
def run_backfill_dates(
    sport_key: str,
    league_slug: str,
    season: str,
    fixtures_file: Path,
    snapshots: List[str],
    markets: List[str],
    regions: str,
    bookmakers: Optional[str],
    outdir: Path,
    cache_ttl: int = 0,
    log_usage: int = 0,
    skip_if_present: int = 1,          # skip call when event@snapshot already has all markets saved
    filter_player_outcomes: int = 1,   # keep Over/Yes for player props; set AGS line=1 when missing
):
    logging.info("Backfill start season=%s league=%s", season, league_slug)
    if not fixtures_file.exists():
        raise FileNotFoundError(f"Fixtures file not found: {fixtures_file}")

    fx = pd.read_csv(fixtures_file)
    if "match_date" not in fx.columns:
        raise ValueError(f"{fixtures_file} must contain a 'match_date' column")
    # unique match days (YYYY-MM-DD)
    days = pd.to_datetime(fx["match_date"], errors="coerce").dropna().dt.strftime("%Y-%m-%d").unique().tolist()
    days = sorted(days)

    client = Client(cache_ttl=cache_ttl, log_usage=bool(int(log_usage)))
    markets_set = set(markets)
    markets_csv = ",".join(markets)

    for snap in snapshots:
        anchor_h = SNAPSHOT_ANCHORS.get(snap)
        if anchor_h is None:
            logging.warning("Unknown snapshot '%s' – skipping", snap)
            continue

        out_path = outdir / f"{snap}_{season}_{league_slug}.csv"

        print(snap, out_path)
        if not out_path.exists():
            pd.DataFrame(columns=EMPTY_COLUMNS).to_csv(out_path, index=False)

        have_index = _load_completed_index(out_path)

        total_rows = 0
        total_events = 0
        skipped_calls = 0
        empty_payloads = 0

        for d in days:
            # Anchor exactly at midnight UTC (like your working approach)
            anchor_iso = f"{d}T00:00:00Z"
            events = client.historical_events_for_iso(sport_key, anchor_iso)

            # keep only events that actually commence on day d (UTC)
            day_events: List[Dict[str, Any]] = []
            for e in events:
                ct = parse_iso_utc(e.get("commence_time") or "")
                if ct and ct.strftime("%Y-%m-%d") == d:
                    day_events.append(e)

            if not day_events:
                continue

            total_events += len(day_events)

            for ev in day_events:
                ko = parse_iso_utc(ev.get("commence_time",""))
                if not ko:
                    continue
                iso_snapshot_time = to_iso_z(ko - timedelta(hours=anchor_h))
                key = (ev["id"], iso_snapshot_time)

                # Skip call if we already have ALL requested markets
                if skip_if_present:
                    have = have_index.get(key, set())
                    if have.issuperset(markets_set):
                        skipped_calls += 1
                        continue

                # ---- Fetch once for all requested markets
                try:
                    payload = client.historical_odds_for_event(
                        sport_key=sport_key,
                        event_id=ev["id"],
                        iso_snapshot_time=iso_snapshot_time,
                        regions=regions,
                        markets_csv=markets_csv,
                        bookmakers=bookmakers,
                        odds_format="decimal",
                        date_format="iso",
                    )
                except requests.HTTPError as e:
                    logging.debug("history miss id=%s snap=%s at=%s: %s", ev.get("id"), snap, iso_snapshot_time, e)
                    continue

                df = _flatten_payload(payload, ev, snap, iso_snapshot_time, season, league_slug)
                if df.empty:
                    empty_payloads += 1
                    continue

                # Optional: limit to Over/Yes for player props and normalize AGS line
                if filter_player_outcomes:
                    mask_player = df["player"].notna()
                    if mask_player.any():
                        keep = (df["direction"].isin(["Over", "Yes"])) & mask_player
                        df = pd.concat([df[keep], df[~mask_player]], ignore_index=True)
                        is_ags = (df["market"] == "player_goal_scorer_anytime") & (df["direction"] == "Yes")
                        df.loc[is_ags & df["line"].isna(), "line"] = 1.0

                # ---- WRITE AFTER EACH EVENT FETCH
                _append_dedup(out_path, df)
                total_rows += len(df)
                print(total_rows)

                # Update “have” index so later iterations can skip
                present_markets = set(df["market"].dropna().unique().tolist())
                if present_markets:
                    have_index[key] = have_index.get(key, set()).union(present_markets)

        logging.info(
            "✅ %s → %s | days=%d events=%d rows=%d empty_payloads=%d skipped_calls=%d",
            snap, out_path, len(days), total_events, total_rows, empty_payloads, skipped_calls
        )

    logging.info("Backfill done.")

# ---------------- CLI ----------------
def parse_args():
    p = argparse.ArgumentParser(description="Historical odds backfill (date → events → odds, per snapshot; writes per event)")
    p.add_argument("--sport-key", required=True)
    p.add_argument("--league-slug", required=True)
    p.add_argument("--season", required=True)
    p.add_argument("--fixtures-file", required=True, type=Path)
    p.add_argument("--snapshots", default="openers,midweek,bet,close")
    p.add_argument("--markets", required=True, help="comma-separated markets")
    p.add_argument("--regions", default="uk,eu,us")
    p.add_argument("--bookmakers", default="")
    p.add_argument("--outdir", default="data/raw/odds", type=Path)
    p.add_argument("--cache-ttl", type=int, default=0, help="HTTP cache TTL seconds (0 = off)")
    p.add_argument("--log-usage", type=int, choices=[0,1], default=0)
    p.add_argument("--skip-if-present", type=int, choices=[0,1], default=1,
                   help="Skip API call when event@snapshot already has all requested markets saved")
    p.add_argument("--filter-player-outcomes", type=int, choices=[0,1], default=1,
                   help="Keep only Over/Yes for player props; set AGS line=1 when missing")
    return p.parse_args()

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    a = parse_args()
    snaps = [s.strip() for s in a.snapshots.split(",") if s.strip()]
    mkts  = [m.strip() for m in a.markets.split(",") if m.strip()]
    run_backfill_dates(
        sport_key=a.sport_key,
        league_slug=a.league_slug,
        season=a.season,
        fixtures_file=a.fixtures_file,
        snapshots=snaps,
        markets=mkts,
        regions=a.regions,
        bookmakers=(a.bookmakers or None),
        outdir=a.outdir,
        cache_ttl=a.cache_ttl,
        log_usage=a.log_usage,
        skip_if_present=a.skip_if_present,
        filter_player_outcomes=a.filter_player_outcomes,
    )

if __name__ == "__main__":
    main()
