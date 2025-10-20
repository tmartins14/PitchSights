# feature_store/possession_split.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple
import argparse
import numpy as np
import pandas as pd
import warnings


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

def _require(df: pd.DataFrame, cols) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

def _ensure_datetime(df: pd.DataFrame, col: str) -> None:
    if col in df.columns and not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col], errors="coerce")

def _build_shifted_rolling_possession(
    results: pd.DataFrame,
    team_stats: pd.DataFrame,
    *,
    date_col: str,
    home_col: str,
    away_col: str,
    match_id_col: Optional[str],
    window_games: int,
) -> pd.DataFrame:
    """
    Add leak-free rolling possession features per team to `results`:

      home_poss_for_wN, home_poss_against_wN, away_poss_for_wN, away_poss_against_wN

    Where possession columns are percentages in [0,100].
    """
    df = results.copy()
    _ensure_datetime(df, date_col)

    # expected columns in team_stats
    H_POSS = "home_possession"
    A_POSS = "away_possession"

    have_any = [c for c in (H_POSS, A_POSS) if c in team_stats.columns]
    if not have_any:
        warnings.warn("team_stats missing possession columns (home_possession/away_possession).")
        # create empty columns so later code still runs
        for side in ("home", "away"):
            df[f"{side}_poss_for_w{window_games}"] = np.nan
            df[f"{side}_poss_against_w{window_games}"] = np.nan
        return df

    _ensure_datetime(team_stats, date_col)

    can_join_on_mid = (
        match_id_col
        and (match_id_col in df.columns)
        and (match_id_col in team_stats.columns)
        and df[match_id_col].notna().any()
        and team_stats[match_id_col].notna().any()
    )

    if can_join_on_mid:
        mapping = df[[match_id_col, date_col, home_col, away_col]].drop_duplicates()
        stats = team_stats.merge(mapping, on=match_id_col, how="inner")
    else:
        if not ({date_col, home_col, away_col} <= set(team_stats.columns)):
            warnings.warn("No valid join key (match_id or date+teams) between team_stats and results; returning unchanged.")
            return df
        stats = team_stats.merge(df[[date_col, home_col, away_col]].drop_duplicates(),
                                 on=[date_col, home_col, away_col], how="inner")
    if stats.empty:
        warnings.warn("Join team_stats↔matches yielded no rows; returning unchanged.")
        return df

    # Build long per-team possession (for/against) for each match row
    long_rows = []
    def g(row, col):  # safe getter
        return row[col] if col in row.index else np.nan

    for _, r in stats.iterrows():
        # Home team row
        long_rows.append({
            "team": r[home_col],
            "date": r[date_col],
            "poss_for": g(r, H_POSS),
            "poss_against": 100.0 - g(r, A_POSS) if A_POSS in stats.columns else np.nan,
        })
        # Away team row
        long_rows.append({
            "team": r[away_col],
            "date": r[date_col],
            "poss_for": g(r, A_POSS),
            "poss_against": 100.0 - g(r, H_POSS) if H_POSS in stats.columns else np.nan,
        })

    long_df = pd.DataFrame(long_rows).dropna(subset=["team", "date"])
    if long_df.empty:
        warnings.warn("No valid (team,date) rows constructed for possession; returning unchanged.")
        return df

    _ensure_datetime(long_df, "date")
    long_df = long_df.sort_values(["team", "date"]).reset_index(drop=True)

    # Coerce to numeric percentages
    for c in ("poss_for", "poss_against"):
        if c in long_df.columns:
            long_df[c] = pd.to_numeric(long_df[c], errors="coerce")

    # Anti-leak shift(1) then rolling mean over window_games
    def _roll_one_team(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        g["poss_for"] = g["poss_for"].shift(1)
        g["poss_against"] = g["poss_against"].shift(1)
        g[f"poss_for_w{window_games}"] = g["poss_for"].rolling(window_games, min_periods=1).mean()
        g[f"poss_against_w{window_games}"] = g["poss_against"].rolling(window_games, min_periods=1).mean()
        return g

    rolled = long_df.groupby("team", group_keys=False).apply(_roll_one_team)
    keep = ["team", "date", f"poss_for_w{window_games}", f"poss_against_w{window_games}"]
    rolled = rolled[keep].drop_duplicates(subset=["team", "date"], keep="last")

    # Merge back as home_/away_*
    left = rolled.rename(columns={"team": home_col, "date": date_col})
    right = rolled.rename(columns={"team": away_col, "date": date_col})

    df = df.merge(left, on=[home_col, date_col], how="left", suffixes=("", "_home"))
    df = df.merge(right, on=[away_col, date_col], how="left", suffixes=("_home", "_away"))

    base = f"_w{window_games}"
    # rename to explicit home/away columns
    if f"poss_for{base}_home" in df.columns:
        df.rename(columns={f"poss_for{base}_home": f"home_poss_for{base}"}, inplace=True)
    if f"poss_against{base}_home" in df.columns:
        df.rename(columns={f"poss_against{base}_home": f"home_poss_against{base}"}, inplace=True)

    if f"poss_for{base}_away" in df.columns:
        df.rename(columns={f"poss_for{base}_away": f"away_poss_for{base}"}, inplace=True)
    if f"poss_against{base}_away" in df.columns:
        df.rename(columns={f"poss_against{base}_away": f"away_poss_against{base}"}, inplace=True)

    return df


# ------------------------------
# Public API
# ------------------------------
def make_possession_split_from_team_stats(
    matches: pd.DataFrame,
    team_stats: pd.DataFrame,
    *,
    home_team_col: str = "home_team",
    away_team_col: str = "away_team",
    date_col: str = "match_date",
    match_id_col: str = "match_id",
    window_games: int = 8,
    home_adv_pp: float = 1.0,   # home advantage in percentage points
) -> pd.DataFrame:
    """
    Compute expected possession per match using leak-free rolling possession.

    Steps:
      1) Build per-team rolling possession (for & against) in [0,100].
      2) Blend for the match:
         exp_pos_home[%] ≈ mean( home_poss_for_wN , 100 - away_poss_for_wN ) + home_adv_pp
         exp_pos_away[%] = 100 - exp_pos_home[%]
      3) Return exp_pos_* in [0,1].

    Returns
      DataFrame with [date_col?, home_team_col, away_team_col, match_id_col?, exp_pos_home, exp_pos_away]
    """
    _require(matches, [home_team_col, away_team_col, date_col])
    df = _build_shifted_rolling_possession(
        results=matches,
        team_stats=team_stats,
        date_col=date_col,
        home_col=home_team_col,
        away_col=away_team_col,
        match_id_col=match_id_col,
        window_games=window_games,
    )

    base = f"_w{window_games}"
    def col(name): return df[name] if name in df.columns else pd.Series(np.nan, index=df.index)

    # Use 'for' series for both sides; build opponent 'against' via (100 - opp_for)
    home_for = col(f"home_poss_for{base}")       # %
    away_for = col(f"away_poss_for{base}")       # %

    exp_home_pct = 0.5 * (home_for + (100.0 - away_for)) + float(home_adv_pp)
    exp_home_pct = exp_home_pct.clip(0.0, 100.0)
    exp_away_pct = 100.0 - exp_home_pct

    out = matches[[home_team_col, away_team_col]].copy()
    if date_col in matches.columns:
        _ensure_datetime(matches, date_col)
        out[date_col] = matches[date_col]
    if match_id_col in matches.columns:
        out[match_id_col] = matches[match_id_col]

    out["exp_pos_home"] = (exp_home_pct / 100.0).astype(float)
    out["exp_pos_away"] = (exp_away_pct / 100.0).astype(float)
    return out


# ------------------------------
# CLI
# ------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute expected possession from team_stats via leak-free rolling windows.")
    parser.add_argument("--matches", required=True, help="Path to matches CSV/Parquet.")
    parser.add_argument("--team-stats", required=True, help="Path to team_stats CSV/Parquet (needs home_possession, away_possession).")
    parser.add_argument("--out", required=True, help="Output path (.parquet preferred).")

    # Schema
    parser.add_argument("--date-col", default="match_date")
    parser.add_argument("--home-team-col", default="home_team")
    parser.add_argument("--away-team-col", default="away_team")
    parser.add_argument("--match-id-col", default="match_id")

    # Params
    parser.add_argument("--window-games", type=int, default=8)
    parser.add_argument("--home-adv", type=float, default=1.0, help="Home advantage in percentage points (e.g., 1.0 → +1pp to home).")

    args = parser.parse_args()

    matches    = _read_table(args.matches)
    team_stats = _read_table(args.team_stats)

    out = make_possession_split_from_team_stats(
        matches,
        team_stats,
        home_team_col=args.home_team_col,
        away_team_col=args.away_team_col,
        date_col=args.date_col,
        match_id_col=args.match_id_col,
        window_games=int(args.window_games),
        home_adv_pp=float(args.home_adv),
    )

    out_path = Path(args.out); out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        out.to_parquet(out_path, index=False)
        print(f"[context] possession_split -> {out_path}")
    except Exception:
        csv_path = out_path.with_suffix(".csv")
        out.to_csv(csv_path, index=False)
        print(f"[warn] Parquet write failed; wrote CSV instead -> {csv_path}")
