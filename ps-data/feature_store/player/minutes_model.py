# feature_store/player/minutes_model.py
# Python 3.9.6
from __future__ import annotations

from pathlib import Path
from typing import Sequence
import argparse
import numpy as np
import pandas as pd


def _read_table(path: str) -> pd.DataFrame:
    p = Path(str(path).strip().strip("'").strip('"'))
    if p.suffix.lower() == ".parquet":
        try:
            return pd.read_parquet(p)
        except Exception:
            return pd.read_csv(p)
    return pd.read_csv(p)

def _ensure_datetime(df: pd.DataFrame, col: str) -> None:
    if col in df.columns and not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col], errors="coerce")


def make_expected_minutes(
    players: pd.DataFrame,
    *,
    player_col: str = "player",
    team_col: str = "team",
    date_col: str = "match_date",
    match_id_col: str = "match_id",
    minutes_col: str = "minutes",
    windows: Sequence[int] = (3, 6, 10),
    ewma_alpha: float = 0.5,     # 0<alpha<=1, higher = more recent emphasis
) -> pd.DataFrame:
    """
    Leak-free rolling minutes + EWMA minutes + starter probability.

    Derived per match:
      started = 1[min >= 60]
    Rolling per player (shift→roll):
      minutes_wN, started_wN, exp_minutes_ewma (recomputed each step, excluding current),
      starter_prob_wN  := rolling mean of started (0/1)
    """
    req = [player_col, team_col, date_col, match_id_col, minutes_col]
    for c in req:
        if c not in players.columns:
            raise ValueError(f"Missing required column: {c}")

    df = players.copy()
    _ensure_datetime(df, date_col)
    df = df.dropna(subset=[player_col, team_col, date_col, match_id_col]).copy()
    df = df.sort_values([player_col, team_col, date_col, match_id_col], kind="mergesort").reset_index(drop=True)

    df["minutes"] = pd.to_numeric(df[minutes_col], errors="coerce")
    df["started"] = (df["minutes"] >= 60).astype(float)
    df["_player_key"] = df[player_col].astype(str) + " || " + df[team_col].astype(str)

    def _roll_one(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values([date_col, match_id_col]).copy()
        # shift to prevent leak
        g["minutes_shift"] = g["minutes"].shift(1)
        g["started_shift"] = g["started"].shift(1)

        # rolling means
        for w in windows:
            g[f"minutes_w{w}"] = g["minutes_shift"].rolling(w, min_periods=1).mean()
            g[f"starter_prob_w{w}"] = g["started_shift"].rolling(w, min_periods=1).mean()

        # EWMA minutes (leak-free): compute cumulatively on shifted minutes
        m = g["minutes_shift"]
        # pandas ewm includes current row; we pass shifted series so it's already excluding.
        g["exp_minutes_ewma"] = m.ewm(alpha=float(ewma_alpha), adjust=False).mean()
        return g

    out = df.groupby("_player_key", group_keys=False).apply(_roll_one).reset_index(drop=True)

    keep = ["_player_key", player_col, team_col, match_id_col, date_col,
            "exp_minutes_ewma"] + [c for c in out.columns if c.startswith(("minutes_w","starter_prob_w"))]
    out = out[keep].rename(columns={"_player_key": "player_key"})
    return out


# ---------- CLI ----------
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Leak-free expected minutes (rolling + EWMA) and starter probability.")
    p.add_argument("--players", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--windows", default="3,6,10")
    p.add_argument("--ewma-alpha", type=float, default=0.5)
    p.add_argument("--player-col", default="player")
    p.add_argument("--team-col", default="team")
    p.add_argument("--date-col", default="match_date")
    p.add_argument("--match-id-col", default="match_id")
    p.add_argument("--minutes-col", default="minutes")
    args = p.parse_args()

    players = _read_table(args.players)
    windows = [int(x) for x in str(args.windows).split(",") if str(x).strip()]

    out = make_expected_minutes(
        players,
        player_col=args.player_col,
        team_col=args.team_col,
        date_col=args.date_col,
        match_id_col=args.match_id_col,
        minutes_col=args.minutes_col,
        windows=windows,
        ewma_alpha=float(args.ewma_alpha),
    )

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    try:
        out.to_parquet(args.out, index=False)
        print(f"[player] minutes_model -> {args.out}")
    except Exception:
        out.to_csv(Path(args.out).with_suffix(".csv"), index=False)
        print("[warn] parquet failed; wrote CSV instead.")
