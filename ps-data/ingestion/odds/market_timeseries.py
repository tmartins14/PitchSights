# ingestion/odds/market_timeseries.py
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List

from ingestion.odds.the_odds_api import run_odds_snapshot

def to_dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)

def daterange(start: datetime, end: datetime, step_hours: int):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(hours=step_hours)

def parse_args():
    p = argparse.ArgumentParser(
        description="Crawl market(s) as a time series (e.g., daily SoT/AGS odds across a season)."
    )
    p.add_argument("--sport-key", default="soccer_epl")
    p.add_argument("--league-slug", default="Premier-League")
    p.add_argument("--season", default="2024-2025")
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD (UTC)")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD (UTC)")
    p.add_argument("--regions", default="uk,eu,us")
    p.add_argument("--bookmakers", default="")
    p.add_argument("--markets", default="player_shots_on_target",
                   help="Comma list, e.g. 'player_shots_on_target' or 'player_goals,player_shots_on_target'")
    p.add_argument("--step-hours", type=int, default=24)
    p.add_argument("--cache-ttl", type=int, default=60*60)
    p.add_argument("--window-max-hours", type=int, default=21*24,
                   help="From each anchor, look ahead this many hours (default 21 days).")
    p.add_argument("--force", action="store_true")
    return p.parse_args()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    a = parse_args()

    markets = [m.strip() for m in a.markets.split(",") if m.strip()]
    start = to_dt(a.start_date)
    end   = to_dt(a.end_date)

    for anchor in daterange(start, end, a.step_hours):
        anchor_tag = anchor.strftime("%Y%m%dT%H%M%SZ")
        outname = f"timeseries_{a.league_slug}_{a.season}_{anchor_tag}_{'-'.join(markets)}.csv"
        outpath = Path("data/raw/odds") / "timeseries"
        outpath.mkdir(parents=True, exist_ok=True)
        outcsv = outpath / outname
        if outcsv.exists() and not a.force:
            logging.info(f"Skip existing {outcsv.name}")
            continue

        # We don't apply a min window (0h). We look ahead up to window_max_hours.
        run_odds_snapshot(
            snapshot_type="timeseries",
            sport_key=a.sport_key,
            league_slug=a.league_slug,
            regions=a.regions,
            bookmakers=(a.bookmakers or None),
            markets=markets,
            cache_ttl=a.cache_ttl,
            window_min_hours=0,
            window_max_hours=a.window_max_hours,
            season=a.season,
            dedup_mode="latest",   # latest at the time of anchor
            as_of=anchor,
            dest_path=outcsv,
        )
