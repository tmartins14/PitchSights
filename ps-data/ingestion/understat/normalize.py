# ingestion/understat/normalize.py
from __future__ import annotations
import os, logging
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw" / "understat"
INTERIM_DIR = DATA_DIR / "interim" / "understat"
INTERIM_DIR.mkdir(parents=True, exist_ok=True)

def normalize_fixtures(season: str, league_slug: str) -> None:
    src = RAW_DIR / f"fixtures_{season}_{league_slug}.csv"
    dst = INTERIM_DIR / "fixtures.csv"
    if not src.exists():
        logging.warning(f"No fixtures at {src}")
        return
    df = pd.read_csv(src)
    if "match_date" in df.columns:
        df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce").dt.date
    order = ["match_id","match_date","start_time","home_team","away_team",
             "home_xG","away_xG","home_score","away_score","match_url","season","league_slug"]
    cols = [c for c in order if c in df.columns] + [c for c in df.columns if c not in order]
    df[cols].to_csv(dst, index=False)
    logging.info(f"✅ Fixtures normalized → {dst}")

def normalize_team_stats(season: str, league_slug: str) -> None:
    src = RAW_DIR / f"team_stats_{season}_{league_slug}.csv"
    dst = INTERIM_DIR / "team_stats.csv"
    if not src.exists():
        logging.warning(f"No team stats at {src}")
        return
    df = pd.read_csv(src)
    for c in ["team_shots","team_shots_on_target","team_goals"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], downcast="integer", errors="coerce")
    for c in ["team_xG"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    order = ["match_id","team_side","team","team_shots","team_shots_on_target","team_goals","team_xG","season","league_slug"]
    cols = [c for c in order if c in df.columns] + [c for c in df.columns if c not in order]
    df[cols].to_csv(dst, index=False)
    logging.info(f"✅ Team stats normalized → {dst}")

def normalize_player_stats(season: str, league_slug: str) -> None:
    src = RAW_DIR / f"player_stats_{season}_{league_slug}.csv"
    dst = INTERIM_DIR / "player_stats.csv"
    if not src.exists():
        logging.warning(f"No player stats at {src}")
        return
    df = pd.read_csv(src)
    nums = ["shots","shots_on_target","goals","assists"]
    floats = ["xG","xA"]
    for c in nums:
        if c in df.columns: df[c] = pd.to_numeric(df[c], downcast="integer", errors="coerce")
    for c in floats:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    order = ["match_id","team_side","player_id","player","shots","shots_on_target","goals","xG","assists","xA","season","league_slug"]
    cols = [c for c in order if c in df.columns] + [c for c in df.columns if c not in order]
    df[cols].to_csv(dst, index=False)
    logging.info(f"✅ Player stats normalized → {dst}")

if __name__ == "__main__":
    league_slug = os.getenv("LEAGUE_SLUG", "Premier-League")
    season_start_year = int(os.getenv("SEASON_START_YEAR", "2024"))
    season = f"{season_start_year}-{season_start_year+1}"
    normalize_fixtures(season, league_slug)
    normalize_team_stats(season, league_slug)
    normalize_player_stats(season, league_slug)
