# ingestion/odds/season_snapshots.py
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional

import pandas as pd

from ingestion.odds.the_odds_api import run_odds_snapshot

WINDOWS: dict = {
    "openers": (96, 168, "earliest"),  # 4–7 days
    "midweek": (48, 72, "earliest"),   # 2–3 days
    "bet":     (3, 24, "earliest"),    # 24–3 hours
    "close":   (0, 3, "latest"),       # last 3 hours
}

def to_dt(s: str) -> datetime:
    # expects YYYY-MM-DD
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)

def daterange(start: datetime, end: datetime, step_hours: int):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(hours=step_hours)

def parse_args():
    p = argparse.ArgumentParser(description="Run season-wide odds snapshots (historical-friendly).")
    p.add_argument("--snapshots", default="openers,midweek,bet,close",
                   help="Comma list from {openers,midweek,bet,close}")
    p.add_argument("--sport-key", default="soccer_epl")
    p.add_argument("--league-slug", default="Premier-League")
    p.add_argument("--season", default="2024-2025")
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD (UTC)")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD (UTC)")
    p.add_argument("--regions", default="uk,eu,us")
    p.add_argument("--bookmakers", default="")
    p.add_argument("--markets", default="h2h,spreads,totals,player_goal_scorer_anytime,player_shots_on_target")
    p.add_argument("--step-hours", type=int, default=24, help="Anchor step in hours (e.g., 24 for daily).")
    p.add_argument("--cache-ttl", type=int, default=6*60*60)
    p.add_argument("--force", action="store_true", help="Overwrite existing files for the same anchor snapshot.")
    return p.parse_args()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    a = parse_args()

    snaps = [s.strip() for s in a.snapshots.split(",") if s.strip()]
    markets = [m.strip() for m in a.markets.split(",") if m.strip()]

    start = to_dt(a.start_date)
    end   = to_dt(a.end_date)

    for anchor in daterange(start, end, a.step_hours):
        anchor_tag = anchor.strftime("%Y%m%dT%H%M%SZ")
        for sname in snaps:
            if sname not in WINDOWS:
                logging.warning(f"Skip unknown snapshot '{sname}'")
                continue
            min_h, max_h, mode = WINDOWS[sname]
            # deterministic historical filename
            outname = f"{sname}_{a.league_slug}_{a.season}_{anchor_tag}.csv"
            outpath = Path("data/raw/odds") / outname
            if outpath.exists() and not a.force:
                logging.info(f"Skip existing {outname}")
                continue

            run_odds_snapshot(
                snapshot_type=sname,
                sport_key=a.sport_key,
                league_slug=a.league_slug,
                regions=a.regions,
                bookmakers=(a.bookmakers or None),
                markets=markets,
                cache_ttl=a.cache_ttl,
                window_min_hours=min_h,
                window_max_hours=max_h,
                season=a.season,
                dedup_mode=mode,
                as_of=anchor,
                dest_path=outpath,
            )
