from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RAW_FBREF = Path("data/raw/fbref")
INT_FBREF = Path("data/interim/fbref")
INT_FBREF.mkdir(parents=True, exist_ok=True)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def normalize_fixtures(raw_path: Path, out_path: Optional[Path] = None) -> pd.DataFrame:
    df = _read_csv(raw_path)
    if df.empty:
        return df
    # light typing/ordering
    order = [
        "match_id", "match_date", "start_time",
        "home_team", "away_team",
        "home_score", "away_score",
        "home_xG", "away_xG",
        "match_url", "season", "league_slug"
    ]
    cols = [c for c in order if c in df.columns] + [c for c in df.columns if c not in order]
    out = df[cols].copy()
    out["match_date"] = pd.to_datetime(out["match_date"], errors="coerce")
    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(out_path, index=False)
    return out


def normalize_team_stats(raw_path: Path, out_path: Optional[Path] = None) -> pd.DataFrame:
    df = _read_csv(raw_path)
    if df.empty:
        return df
    # keep original home_/away_ fields from scraper; add types
    cast_int = [
        "home_shots_on_target","away_shots_on_target","home_shots","away_shots",
        "home_saves","away_saves","home_saves_faced","away_saves_faced",
        "home_passing_accuracy","away_passing_accuracy"
    ]
    for c in cast_int:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
    return df


def normalize_player_stats(raw_path: Path, out_path: Optional[Path] = None) -> pd.DataFrame:
    df = _read_csv(raw_path)
    if df.empty:
        return df
    # ensure numeric where expected
    numeric_like = [
        "minutes","goals","shots","shots_on_target","xg","npxg","xg_assist","sca","gca","pens_att",
        "miscontrols","dispossessed","passes_received","progressive_passes_received",
        "take_ons","take_ons_won","take_ons_tackled","touches_att_3rd","touches_att_pen_area"
    ]
    for c in numeric_like:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
    return df


if __name__ == "__main__":
    # Convenience CLI: rebuild all interim from latest raw files (if present)
    fixtures_latest = sorted(RAW_FBREF.glob("fixtures_*.csv"))[-1:] or []
    team_latest     = sorted(RAW_FBREF.glob("team_stats_*.csv"))[-1:] or []
    player_latest   = sorted(RAW_FBREF.glob("player_stats_*.csv"))[-1:] or []

    if fixtures_latest:
        f = fixtures_latest[-1]
        out = INT_FBREF / "fixtures.csv"
        normalize_fixtures(f, out)
        logging.info(f"✅ fixtures → {out}")

    if team_latest:
        t = team_latest[-1]
        out = INT_FBREF / "team_stats.csv"
        normalize_team_stats(t, out)
        logging.info(f"✅ team_stats → {out}")

    if player_latest:
        p = player_latest[-1]
        out = INT_FBREF / "player_stats.csv"
        normalize_player_stats(p, out)
        logging.info(f"✅ player_stats → {out}")
