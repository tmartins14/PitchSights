# ingestion/understat/fetch_team_stats.py
from __future__ import annotations
import os, asyncio, logging
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
from ingestion.understat.client_async import make_client, close_client, get_match_shots, season_label

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw" / "understat"
RAW_DIR.mkdir(parents=True, exist_ok=True)

SOT_RESULTS = {"Goal", "SavedShot"}

def _team_aggregate(shots: List[Dict[str, Any]]) -> Dict[str, Any]:
    shots_count = len(shots)
    sot_count = sum(1 for s in shots if (s.get("result") in SOT_RESULTS))
    goals = sum(1 for s in shots if (s.get("result") == "Goal" or s.get("isGoal") is True))
    xg = 0.0
    for s in shots:
        try:
            xg += float(s.get("xG") or 0.0)
        except Exception:
            pass
    return dict(team_shots=shots_count, team_shots_on_target=sot_count, team_goals=goals, team_xG=xg)

async def _one_match(us, match_id: str, home_team: str, away_team: str, season: Optional[str], league_slug: Optional[str]) -> Optional[pd.DataFrame]:
    data = await get_match_shots(us, match_id)
    if not data:
        return None
    h = data.get("h") or []
    a = data.get("a") or []
    h_agg = _team_aggregate(h)
    a_agg = _team_aggregate(a)
    rows = [
        {"match_id": match_id, "team_side": "home", "team": home_team, **h_agg},
        {"match_id": match_id, "team_side": "away", "team": away_team, **a_agg},
    ]
    df = pd.DataFrame(rows)
    if season:
        df["season"] = season
    if league_slug:
        df["league_slug"] = league_slug
    return df

async def main():
    league_slug = os.getenv("LEAGUE_SLUG", "Premier-League")
    season_start_year = int(os.getenv("SEASON_START_YEAR", "2024"))
    season = season_label(season_start_year)
    throttle = float(os.getenv("THROTTLE", "1.5"))

    fixtures_file = RAW_DIR / f"fixtures_{season}_{league_slug}.csv"
    out_file = RAW_DIR / f"team_stats_{season}_{league_slug}.csv"
    if not fixtures_file.exists():
        logging.error(f"No fixtures at {fixtures_file}. Run fetch_fixtures first.")
        raise SystemExit(1)

    fixtures = pd.read_csv(fixtures_file)
    played = fixtures.dropna(subset=["home_score", "away_score"])
    logging.info(f"{len(played)} played matches found.")

    # Incremental skip
    done_ids = set()
    if out_file.exists():
        try:
            prev = pd.read_csv(out_file, usecols=["match_id"])
            done_ids = set(prev["match_id"].astype(str).unique())
            logging.info(f"Skipping {len(done_ids)} previously processed matches.")
        except Exception:
            pass

    session, us = await make_client()
    frames: List[pd.DataFrame] = []
    try:
        for _, r in played.iterrows():
            mid = str(r["match_id"])
            if mid in done_ids:
                continue
            df = await _one_match(us, mid, r["home_team"], r["away_team"], season, league_slug)
            if df is not None and not df.empty:
                frames.append(df)
            await asyncio.sleep(throttle)

    finally:
        await close_client(session)

    out_df = pd.concat([pd.read_csv(out_file)] + frames, ignore_index=True) if (out_file.exists() and frames) else (
             pd.concat(frames, ignore_index=True) if frames else (
             pd.read_csv(out_file) if out_file.exists() else pd.DataFrame()))
    if not out_df.empty:
        out_df.to_csv(out_file, index=False)
    logging.info(f"✅ Wrote team stats → {out_file} ({len(out_df)} rows)")

if __name__ == "__main__":
    asyncio.run(main())
