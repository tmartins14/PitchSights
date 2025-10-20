# feature_store/opponent_allowed.py
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional
import argparse
import numpy as np
import pandas as pd


# ---------- utils ----------
def _read_table(path: str) -> pd.DataFrame:
    p = Path(path.strip().strip("'").strip('"'))
    if p.suffix.lower() == ".parquet":
        try:
            return pd.read_parquet(p)
        except Exception:
            return pd.read_csv(p)
    return pd.read_csv(p)

def _require(df: pd.DataFrame, cols: Iterable[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

def _ensure_datetime(df: pd.DataFrame, col: str) -> None:
    if col in df.columns and not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col], errors="coerce")

def _safe_div(num: pd.Series, den: pd.Series, eps: float = 1e-9) -> pd.Series:
    return pd.to_numeric(num, errors="coerce") / (pd.to_numeric(den, errors="coerce") + eps)


# ---------- core ----------
def make_allowed_rolling(
    matches: pd.DataFrame,
    team_stats: pd.DataFrame,
    *,
    date_col: str = "match_date",
    home_team_col: str = "home_team",
    away_team_col: str = "away_team",
    match_id_col: str = "match_id",
    windows: List[int] = (3, 6, 10),
) -> pd.DataFrame:
    """
    Leak-free rolling *allowed* metrics per team/date.
    Derived from team_stats by flipping to opponent's perspective.

    For each team/date compute (shifted → rolling mean):
      allowed_shots       := shots_against
      allowed_sot         := sot_against
      allowed_possession  := opponent possession %  (= poss_against)
      allowed_shots_per_poss := shots_per_poss_against
      allowed_sot_rate        := sot_rate_against

    Returns: team, date, allowed_*_w{N}
    """
    # Reuse the builder from rolling_team_stats to avoid duplication
    from feature_store.team.rolling_team_stats import make_team_rolling as _base

    rolled = _base(
        matches, team_stats,
        date_col=date_col,
        home_team_col=home_team_col,
        away_team_col=away_team_col,
        match_id_col=match_id_col,
        windows=windows,
    )

    # Map to allowed_* names (these exist because base computed *_against and derived rates)
    out = rolled.copy()
    rename_map = {}
    for w in windows:
        rename_map.update({
            f"shots_against_w{w}":           f"allowed_shots_w{w}",
            f"sot_against_w{w}":             f"allowed_sot_w{w}",
            f"poss_against_w{w}":            f"allowed_possession_w{w}",
            f"shots_per_poss_against_w{w}":  f"allowed_shots_per_poss_w{w}",
            f"sot_rate_against_w{w}":        f"allowed_sot_rate_w{w}",
        })
    out = out.rename(columns=rename_map)

    keep = ["team","date"] + list(rename_map.values())
    return out[keep].reset_index(drop=True)


def make_match_level_opponent_allowed(
    matches: pd.DataFrame,
    allowed_team_table: pd.DataFrame,
    *,
    date_col: str = "match_date",
    home_team_col: str = "home_team",
    away_team_col: str = "away_team",
) -> pd.DataFrame:
    """
    Turn team-level allowed table into match-level features:
      home_opp_* = away team's allowed_*
      away_opp_* = home team's allowed_*
    """
    _ensure_datetime(matches, date_col)
    _ensure_datetime(allowed_team_table, "date")

    left  = allowed_team_table.rename(columns={"team": away_team_col, "date": date_col})
    right = allowed_team_table.rename(columns={"team": home_team_col, "date": date_col})

    df = matches[[date_col, home_team_col, away_team_col, "match_id"]].copy()

    # Merge for home_opp_* (opponent = away team)
    df = df.merge(left, on=[away_team_col, date_col], how="left", suffixes=("", "_homeopp"))
    # Merge for away_opp_* (opponent = home team)
    df = df.merge(right, on=[home_team_col, date_col], how="left", suffixes=("_homeopp", "_awayopp"))

    # Rename cols to clear home_opp_ / away_opp_
    def _relabel(prefix_from: str, prefix_to: str):
        ren = {}
        for c in df.columns:
            if c.startswith(prefix_from):
                ren[c] = c.replace(prefix_from, prefix_to, 1)
        if ren:
            df.rename(columns=ren, inplace=True)

    _relabel("allowed_", "home_opp_")      # from first merge (away team perspective -> home opponent)
    _relabel("allowed__awayopp", "away_opp_")  # from second merge; double underscore because of suffixing

    # Clean any remaining suffix leftovers
    for c in list(df.columns):
        if c.endswith("_homeopp"):
            df.rename(columns={c: c.replace("_homeopp","")}, inplace=True)
        if c.endswith("_awayopp"):
            df.rename(columns={c: c.replace("_awayopp","")}, inplace=True)

    return df


# ---------- CLI ----------
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build leak-free rolling opponent allowed metrics (team & match-level).")
    p.add_argument("--matches", required=True)
    p.add_argument("--team-stats", required=True)
    p.add_argument("--out-team", required=True, help="Team-level allowed table (team,date,allowed_*_wN)")
    p.add_argument("--out-match", required=False, help="Match-level opponent allowed (home_opp_*, away_opp_*)")
    p.add_argument("--date-col", default="match_date")
    p.add_argument("--home-team-col", default="home_team")
    p.add_argument("--away-team-col", default="away_team")
    p.add_argument("--match-id-col", default="match_id")
    p.add_argument("--windows", default="3,6,10")
    args = p.parse_args()

    matches = _read_table(args.matches)
    team_stats = _read_table(args.team_stats)
    windows = [int(x) for x in str(args.windows).split(",") if str(x).strip()]

    team_allowed = make_allowed_rolling(
        matches, team_stats,
        date_col=args.date_col,
        home_team_col=args.home_team_col,
        away_team_col=args.away_team_col,
        match_id_col=args.match_id_col,
        windows=windows,
    )

    Path(args.out_team).parent.mkdir(parents=True, exist_ok=True)
    try:
        team_allowed.to_parquet(args.out_team, index=False)
        print(f"[team] opponent_allowed (team) -> {args.out_team}")
    except Exception:
        team_allowed.to_csv(Path(args.out_team).with_suffix(".csv"), index=False)
        print("[warn] parquet failed; wrote CSV")

    if args.out_match:
        match_allowed = make_match_level_opponent_allowed(
            matches, team_allowed,
            date_col=args.date_col,
            home_team_col=args.home_team_col,
            away_team_col=args.away_team_col,
        )
        try:
            Path(args.out_match).parent.mkdir(parents=True, exist_ok=True)
            match_allowed.to_parquet(args.out_match, index=False)
            print(f"[team] opponent_allowed (match) -> {args.out_match}")
        except Exception:
            match_allowed.to_csv(Path(args.out_match).with_suffix(".csv"), index=False)
            print("[warn] parquet failed; wrote CSV")
