# feature_store/team_style.py
from __future__ import annotations

from pathlib import Path
from typing import List
import argparse
import numpy as np
import pandas as pd


# ------------------------------
# Utilities
# ------------------------------
def _read_table(path: str) -> pd.DataFrame:
    p = Path(path.strip().strip("'").strip('"'))
    if p.suffix.lower() == ".parquet":
        try:
            return pd.read_parquet(p)
        except Exception:
            return pd.read_csv(p)
    return pd.read_csv(p)

def _require(df: pd.DataFrame, cols: List[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

def _ensure_datetime(df: pd.DataFrame, col: str) -> None:
    if col in df.columns and not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col], errors="coerce")

def _safe_div(num: pd.Series, den: pd.Series, eps: float = 1e-9) -> pd.Series:
    return pd.to_numeric(num, errors="coerce") / (pd.to_numeric(den, errors="coerce") + eps)


# ------------------------------
# Build long per-team rows using matches to supply team names
# ------------------------------
def _team_long_from_team_stats_with_matches(
    matches: pd.DataFrame,
    team_stats: pd.DataFrame,
    *,
    date_col: str,
    home_team_col: str,
    away_team_col: str,
    match_id_col: str,
) -> pd.DataFrame:
    """
    Expand team_stats rows (home/away) into per-team rows using team names from matches.
    Requires a reliable join on match_id because team_stats lacks team columns.
    Expected in team_stats (box-score style):
      - home_shots, away_shots
      - home_shots_on_target, away_shots_on_target
      - home_possession, away_possession   (percent [0,100])
    """
    # ensure join keys exist
    _require(matches, [match_id_col, date_col, home_team_col, away_team_col])
    _require(team_stats, [match_id_col])

    # bring teams & date onto team_stats
    mcols = [match_id_col, date_col, home_team_col, away_team_col]
    ts = team_stats.merge(matches[mcols].drop_duplicates(), on=match_id_col, how="inner")
    if ts.empty:
        raise ValueError("Join on match_id produced no rows; ensure team_stats and matches share the same match_id values.")

    # expected stat columns
    H_SHOTS = "home_shots"; A_SHOTS = "away_shots"
    H_SOT   = "home_shots_on_target"; A_SOT = "away_shots_on_target"
    H_POSS  = "home_possession"; A_POSS = "away_possession"

    need_any = [H_SHOTS, A_SHOTS, H_SOT, A_SOT, H_POSS, A_POSS]
    if not any(c in ts.columns for c in need_any):
        raise ValueError("team_stats missing required columns: "
                         "home/away shots, shots_on_target, possession")

    # types & ordering
    _ensure_datetime(ts, date_col)
    ts = ts.sort_values([date_col, match_id_col]).reset_index(drop=True)

    def g(row: pd.Series, col: str) -> float:
        return float(row[col]) if col in row.index and pd.notna(row[col]) else np.nan

    rows = []
    for _, r in ts.iterrows():
        # Home team row
        rows.append({
            "team": r[home_team_col], "date": r[date_col],
            "shots_for": g(r, H_SHOTS), "shots_against": g(r, A_SHOTS),
            "sot_for": g(r, H_SOT),     "sot_against": g(r, A_SOT),
            "poss_for": g(r, H_POSS),   # %
        })
        # Away team row
        rows.append({
            "team": r[away_team_col], "date": r[date_col],
            "shots_for": g(r, A_SHOTS), "shots_against": g(r, H_SHOTS),
            "sot_for": g(r, A_SOT),     "sot_against": g(r, H_SOT),
            "poss_for": g(r, A_POSS),   # %
        })

    long_df = pd.DataFrame(rows).dropna(subset=["team", "date"])
    _ensure_datetime(long_df, "date")
    long_df = long_df.sort_values(["team", "date"]).reset_index(drop=True)

    # numeric coercion
    for c in ["shots_for","shots_against","sot_for","sot_against","poss_for"]:
        if c in long_df.columns:
            long_df[c] = pd.to_numeric(long_df[c], errors="coerce")

    return long_df


# ------------------------------
# Core: leak-free rolling style features (team-level)
# ------------------------------
def make_team_style_rolling(
    matches: pd.DataFrame,
    team_stats: pd.DataFrame,
    *,
    date_col: str = "match_date",
    home_team_col: str = "home_team",
    away_team_col: str = "away_team",
    match_id_col: str = "match_id",
    window_games: int = 8,
) -> pd.DataFrame:
    """
    Returns per-team snapshots (team×date) with shifted-rolling play-style features (anti-leak).
    All suffixes get `_w{N}` for the window size N.

    Features (rolled means unless specified):
      - shots_per_poss_for_wN        = shots_for / (poss_for/100)
      - shots_per_poss_against_wN    = shots_against / (opp_poss/100) with opp_poss = 100 - poss_for
      - sot_rate_for_wN              = sot_for / shots_for
      - sot_rate_against_wN          = sot_against / shots_against
      - poss_for_wN                  = possession percentage [0,100]
      - poss_std_wN                  = rolling std of poss_for (consistency)
    """
    if window_games <= 0:
        raise ValueError("window_games must be > 0")

    long_df = _team_long_from_team_stats_with_matches(
        matches, team_stats,
        date_col=date_col,
        home_team_col=home_team_col,
        away_team_col=away_team_col,
        match_id_col=match_id_col,
    )

    poss_frac = (long_df["poss_for"] / 100.0).clip(0.0, 1.0)
    opp_poss_frac = (1.0 - poss_frac).clip(0.0, 1.0)

    long_df["shots_per_poss_for"]     = _safe_div(long_df["shots_for"], poss_frac)
    long_df["shots_per_poss_against"] = _safe_div(long_df["shots_against"], opp_poss_frac)
    long_df["sot_rate_for"]           = _safe_div(long_df["sot_for"], long_df["shots_for"])
    long_df["sot_rate_against"]       = _safe_div(long_df["sot_against"], long_df["shots_against"])

    feat_base = [
        "shots_per_poss_for",
        "shots_per_poss_against",
        "sot_rate_for",
        "sot_rate_against",
        "poss_for",
    ]

    def _roll_one_team(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        # anti-leak shift
        for c in feat_base:
            g[c] = g[c].shift(1)
        # rolling mean
        for c in feat_base:
            g[f"{c}_w{window_games}"] = g[c].rolling(window_games, min_periods=1).mean()
        # rolling std for possession consistency
        g[f"poss_std_w{window_games}"] = g["poss_for"].rolling(window_games, min_periods=2).std()
        return g

    rolled = long_df.groupby("team", group_keys=False).apply(_roll_one_team)
    keep_cols = ["team", "date"] + [f"{c}_w{window_games}" for c in feat_base] + [f"poss_std_w{window_games}"]
    rolled = rolled[keep_cols].drop_duplicates(subset=["team","date"], keep="last").reset_index(drop=True)

    return rolled


# ------------------------------
# Merge rolled style into matches (home/away)
# ------------------------------
def make_match_style_features(
    matches: pd.DataFrame,
    team_stats: pd.DataFrame,
    *,
    date_col: str = "match_date",
    home_team_col: str = "home_team",
    away_team_col: str = "away_team",
    match_id_col: str = "match_id",
    window_games: int = 8,
) -> pd.DataFrame:
    """
    Produce match-level style features by merging rolled team style onto fixtures.
    Requires a join via match_id because team_stats has no team columns.
    """
    _require(matches, [date_col, home_team_col, away_team_col, match_id_col])

    # Build team-level rolled style (teams & dates derived via matches join)
    rolled = make_team_style_rolling(
        matches, team_stats,
        date_col=date_col,
        home_team_col=home_team_col,
        away_team_col=away_team_col,
        match_id_col=match_id_col,
        window_games=window_games,
    )

    _ensure_datetime(matches, date_col)
    left = rolled.rename(columns={"team": home_team_col, "date": date_col})
    right = rolled.rename(columns={"team": away_team_col, "date": date_col})

    df = matches[[date_col, home_team_col, away_team_col, match_id_col]].copy()

    df = df.merge(left, on=[home_team_col, date_col], how="left", suffixes=("", "_home"))
    df = df.merge(right, on=[away_team_col, date_col], how="left", suffixes=("_home", "_away"))

    base = f"_w{window_games}"
    feat_cols = [
        "shots_per_poss_for",
        "shots_per_poss_against",
        "sot_rate_for",
        "sot_rate_against",
        "poss_for",
        "poss_std",
    ]
    for f in feat_cols:
        col_h = f"{f}{base}_home"
        col_a = f"{f}{base}_away"
        if col_h in df.columns:
            df.rename(columns={col_h: f"home_{f}{base}"}, inplace=True)
        if col_a in df.columns:
            df.rename(columns={col_a: f"away_{f}{base}"}, inplace=True)

    return df


# ------------------------------
# CLI
# ------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build leak-free match-level team style features (requires match_id join).")
    parser.add_argument("--matches", required=True, help="Path to fixtures CSV/Parquet (must include match_id).")
    parser.add_argument("--team-stats", required=True, help="Path to team_stats CSV/Parquet (box-score per match).")
    parser.add_argument("--out", required=True, help="Output path (.parquet preferred).")

    # Schema
    parser.add_argument("--date-col", default="match_date")
    parser.add_argument("--home-team-col", default="home_team")
    parser.add_argument("--away-team-col", default="away_team")
    parser.add_argument("--match-id-col", default="match_id")

    # Params
    parser.add_argument("--window-games", type=int, default=8)

    args = parser.parse_args()

    matches = _read_table(args.matches)
    team_stats = _read_table(args.team_stats)

    out = make_match_style_features(
        matches,
        team_stats,
        date_col=args.date_col,
        home_team_col=args.home_team_col,
        away_team_col=args.away_team_col,
        match_id_col=args.match_id_col,
        window_games=int(args.window_games),
    )

    out_path = Path(args.out); out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        out.to_parquet(out_path, index=False)
        print(f"[context] team_style (match-level) -> {out_path}")
    except Exception:
        csv_path = out_path.with_suffix(".csv")
        out.to_csv(csv_path, index=False)
        print(f"[warn] Parquet write failed; wrote CSV instead -> {csv_path}")
