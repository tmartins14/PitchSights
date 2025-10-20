# ingestion/understat/fetch_match_stats.py
"""
Build per-match stats from Understat shots:
- totals per side: shots, goals, xG
- merges basic context from fixtures_latest (home/away, date)

Writes:
  data/raw/understat/match_stats_latest.csv
"""

import asyncio
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

import aiohttp
import pandas as pd
from understat import Understat

from ingestion.utils import write_raw

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
RAW_DIR = Path("data/raw/understat")
RAW_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_FIXTURES = RAW_DIR / "fixtures.csv"
OUT_PATH = RAW_DIR / "match_stats.csv"


async def _fetch_one_match(ust: Understat, match_id: int) -> Optional[Dict[str, Any]]:
    try:
        shots = await ust.get_match_shots(match_id)
        # shots: list of dicts with keys like 'xG', 'result', 'h_a'
        home_shots = sum(1 for s in shots if s.get("h_a") == "h")
        away_shots = sum(1 for s in shots if s.get("h_a") == "a")
        home_goals = sum(1 for s in shots if s.get("h_a") == "h" and s.get("result") == "Goal")
        away_goals = sum(1 for s in shots if s.get("h_a") == "a" and s.get("result") == "Goal")
        home_xg = sum(float(s.get("xG", 0) or 0.0) for s in shots if s.get("h_a") == "h")
        away_xg = sum(float(s.get("xG", 0) or 0.0) for s in shots if s.get("h_a") == "a")

        return {
            "match_id": match_id,
            "home_shots": home_shots,
            "away_shots": away_shots,
            "home_goals": home_goals,
            "away_goals": away_goals,
            "home_xg": home_xg,
            "away_xg": away_xg,
        }
    except Exception as e:
        logging.warning(f"match_id={match_id} failed: {e}")
        return None


async def _fetch_many(match_ids: List[int], concurrency: int = 6) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    total = len(match_ids)
    done = 0

    sem = asyncio.Semaphore(concurrency)
    async with aiohttp.ClientSession() as session:
        ust = Understat(session)

        async def worker(mid: int):
            async with sem:
                rec = await _fetch_one_match(ust, mid)
                if rec:
                    results.append(rec)
                    done += 1

                    print(f'Finished {done}/{total}')


        await asyncio.gather(*[worker(int(mid)) for mid in match_ids])
    return results


def _event_loop():
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def fetch_match_stats(fixtures_csv: Path = DEFAULT_FIXTURES) -> pd.DataFrame:
    """
    Read fixture list (to get Understat match ids), fetch match shots, aggregate totals.
    Returns DataFrame; also merges home/away/date if present in fixtures.
    """
    if not fixtures_csv.exists():
        logging.error(f"No fixtures file at {fixtures_csv}. Run the Understat fixtures step first.")
        return pd.DataFrame()

    fixtures = pd.read_csv(fixtures_csv)

    total = len(fixtures)


    # Accept common id columns: 'id' (Understat default) or 'match_id'
    id_col = None
    for c in ["id", "match_id", "understat_match_id"]:
        if c in fixtures.columns:
            id_col = c
            break
    if id_col is None:
        logging.error("No match id column found in fixtures (expected 'id' or 'match_id').")
        return pd.DataFrame()

    match_ids = (
        fixtures[id_col]
        .dropna()
        .astype(int)
        .drop_duplicates()
        .tolist()
    )
    if not match_ids:
        logging.warning("No match ids to fetch.")
        return pd.DataFrame()

    logging.info(f"Fetching Understat shots for {len(match_ids)} matches…")
    loop = _event_loop()
    records = loop.run_until_complete(_fetch_many(match_ids, concurrency=6))
    if not records:
        return pd.DataFrame()

    mdf = pd.DataFrame.from_records(records)

    # Merge basic context if available
    keep_cols = [c for c in ["match_date", "home_team", "away_team", "season", "league_slug"] if c in fixtures.columns]
    if keep_cols:
        # Prefer 'match_id' merge key; if fixtures uses 'id', rename for merge only
        fx = fixtures.copy()
        if "match_id" not in fx.columns and "id" in fx.columns:
            fx = fx.rename(columns={"id": "match_id"})
        mdf = mdf.merge(fx[["match_id"] + keep_cols].drop_duplicates("match_id"), on="match_id", how="left")

    return mdf


if __name__ == "__main__":
    df = fetch_match_stats(DEFAULT_FIXTURES)
    wrote = write_raw(df, OUT_PATH)
    logging.info(f"✅ Saved {len(df)} match rows → {OUT_PATH} ({'updated' if wrote else 'unchanged'})")
