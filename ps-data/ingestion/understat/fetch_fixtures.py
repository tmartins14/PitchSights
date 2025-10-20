# ingestion/understat/fetch_fixtures.py
from __future__ import annotations
import os
from pathlib import Path
import logging
import asyncio
import pandas as pd

from ingestion.understat.client_async import make_client, close_client, get_league_fixtures, coerce_fixture_row, season_label

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw" / "understat"
RAW_DIR.mkdir(parents=True, exist_ok=True)

async def fetch_fixtures(league_slug: str, season_start_year: int) -> pd.DataFrame:
    season = season_label(season_start_year)
    session, us = await make_client()
    try:
        raw = await get_league_fixtures(us, league_slug, season_start_year)
        rows = [coerce_fixture_row(m, season, league_slug) for m in (raw or [])]
        df = pd.DataFrame(rows)
        # dtypes
        for col in ["home_xG", "away_xG"]:
            if col in df: df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in ["home_score", "away_score"]:
            if col in df: df[col] = pd.to_numeric(df[col], downcast="integer", errors="coerce")
        return df
    finally:
        await close_client(session)

if __name__ == "__main__":
    league_slug = os.getenv("LEAGUE_SLUG", "Premier-League")
    season_start_year = int(os.getenv("SEASON_START_YEAR", "2024"))
    season = season_label(season_start_year)

    df = asyncio.run(fetch_fixtures(league_slug, season_start_year))
    out = RAW_DIR / f"fixtures_{season}_{league_slug}.csv"
    df.to_csv(out, index=False)
    logging.info(f"✅ Saved {len(df)} fixtures → {out}")
