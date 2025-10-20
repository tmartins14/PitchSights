# feature_store/rolling_team_stats.py
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List
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


# ---------- core builders ----------
def make_team_rolling(
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
    Leak-free rolling team features (for & against). One row per (team, date).
    Computes:
      - shots_for, shots_against
      - sot_for,   sot_against
      - poss_for [0..100], poss_against = 100 - poss_for
      - shots_per_poss_for, shots_per_poss_against
      - sot_rate_for, sot_rate_against
    Then: shift(1), rolling mean for each window.

    Returns columns:
      team, date, <feature>_w{N} for each N in windows
    """
    _require(matches, [match_id_col, date_col, home_team_col, away_team_col])
    _require(team_stats, [match_id_col])

    # bring ids onto team_stats
    id_cols = [match_id_col, date_col, home_team_col, away_team_col]
    ts = team_stats.merge(matches[id_cols].drop_duplicates(), on=match_id_col, how="inner")
    if ts.empty:
        raise ValueError("Join on match_id produced no rows; check match_id alignment.")

    # expected team_stats columns
    H_SHOTS, A_SHOTS = "home_shots", "away_shots"
    H_SOT,   A_SOT   = "home_shots_on_target", "away_shots_on_target"
    H_POSS,  A_POSS  = "home_possession", "away_possession"

    for c in [H_SHOTS, A_SHOTS, H_SOT, A_SOT, H_POSS, A_POSS]:
        if c not in ts.columns:
            ts[c] = np.nan  # tolerate partial schemas

    _ensure_datetime(ts, date_col)
    ts = ts.sort_values([date_col, match_id_col]).reset_index(drop=True)

    rows = []
    def g(r, c): return float(r[c]) if c in r.index and pd.notna(r[c]) else np.nan

    for _, r in ts.iterrows():
        # home row
        rows.append({
            "team": r[home_team_col], "date": r[date_col],
            "shots_for": g(r, H_SHOTS), "shots_against": g(r, A_SHOTS),
            "sot_for": g(r, H_SOT),     "sot_against": g(r, A_SOT),
            "poss_for": g(r, H_POSS),
        })
        # away row
        rows.append({
            "team": r[away_team_col], "date": r[date_col],
            "shots_for": g(r, A_SHOTS), "shots_against": g(r, H_SHOTS),
            "sot_for": g(r, A_SOT),     "sot_against": g(r, H_SOT),
            "poss_for": g(r, A_POSS),
        })

    df = pd.DataFrame(rows).dropna(subset=["team", "date"]).copy()
    _ensure_datetime(df, "date")
    df = df.sort_values(["team", "date"]).reset_index(drop=True)

    for c in ["shots_for","shots_against","sot_for","sot_against","poss_for"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # derived
    poss_frac = (df["poss_for"] / 100.0).clip(0.0, 1.0)
    opp_poss_frac = (1.0 - poss_frac).clip(0.0, 1.0)
    df["poss_against"] = (100.0 - df["poss_for"]).clip(0.0, 100.0)
    df["shots_per_poss_for"]     = _safe_div(df["shots_for"], poss_frac)
    df["shots_per_poss_against"] = _safe_div(df["shots_against"], opp_poss_frac)
    df["sot_rate_for"]           = _safe_div(df["sot_for"], df["shots_for"])
    df["sot_rate_against"]       = _safe_div(df["sot_against"], df["shots_against"])

    base_feats = [
        "shots_for","shots_against","sot_for","sot_against",
        "poss_for","poss_against",
        "shots_per_poss_for","shots_per_poss_against",
        "sot_rate_for","sot_rate_against",
    ]

    def _roll_one(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        # anti-leak
        for c in base_feats:
            g[c] = g[c].shift(1)
        # roll per window
        for w in windows:
            for c in base_feats:
                g[f"{c}_w{w}"] = g[c].rolling(w, min_periods=1).mean()
        return g

    rolled = df.groupby("team", group_keys=False).apply(_roll_one)
    keep = ["team","date"] + [f"{c}_w{w}" for w in windows for c in base_feats]
    return rolled[keep].drop_duplicates(subset=["team","date"], keep="last").reset_index(drop=True)


# ---------- CLI ----------
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build leak-free rolling team stats (for & against).")
    p.add_argument("--matches", required=True)
    p.add_argument("--team-stats", required=True)
    p.add_argument("--out", required=True)  # team-level table
    p.add_argument("--date-col", default="match_date")
    p.add_argument("--home-team-col", default="home_team")
    p.add_argument("--away-team-col", default="away_team")
    p.add_argument("--match-id-col", default="match_id")
    p.add_argument("--windows", default="3,6,10")
    args = p.parse_args()

    matches = _read_table(args.matches)
    team_stats = _read_table(args.team_stats)
    windows = [int(x) for x in str(args.windows).split(",") if str(x).strip()]

    out = make_team_rolling(
        matches, team_stats,
        date_col=args.date_col,
        home_team_col=args.home_team_col,
        away_team_col=args.away_team_col,
        match_id_col=args.match_id_col,
        windows=windows,
    )

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    try:
        out.to_parquet(args.out, index=False)
        print(f"[team] rolling_team_stats -> {args.out}")
    except Exception:
        out.to_csv(Path(args.out).with_suffix(".csv"), index=False)
        print("[warn] parquet failed; wrote CSV")
