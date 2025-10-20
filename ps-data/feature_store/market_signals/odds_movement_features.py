#!/usr/bin/env python3
"""
Odds Movement Feature Engineering
---------------------------------
Generates bookmaker odds movement features across all snapshots
(openers, midweek, bet, close).

Usage Example (Makefile):
    make odds-features LEAGUE=premier_league SEASON=2024-2025
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from scipy.stats import zscore
import warnings


# ============================================================
# Utility Functions
# ============================================================

def _read_table(path):
    """Read CSV or Parquet file automatically."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    if path.suffix == ".csv":
        return pd.read_csv(path)
    elif path.suffix == ".parquet":
        return pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")

def safe_divide(a, b, eps=1e-9):
    """Avoid division by zero."""
    return np.divide(a, b + eps)


# ============================================================
# Core Feature Function
# ============================================================

def create_odds_movement_features(df_open, df_mid, df_bet, df_close):
    keys = ["event_id", "bookmaker", "market", "player", "direction"]

    def rename_with_prefix(df, prefix):
        rename_map = {
            "odds": f"odds_{prefix}",
            "prob": f"prob_{prefix}",
            "snapshot_time": f"snapshot_time_{prefix}"
        }
        return df.rename(columns=rename_map)

    # Rename and merge
    df_open = rename_with_prefix(df_open, "open")
    df_mid = rename_with_prefix(df_mid, "mid")
    df_bet = rename_with_prefix(df_bet, "bet")
    df_close = rename_with_prefix(df_close, "close")

    df = df_open.merge(df_mid, on=keys, how="outer")
    df = df.merge(df_bet, on=keys, how="outer")
    df = df.merge(df_close, on=keys, how="outer")

    # Define snapshot intervals
    intervals = [
        ("open", "mid"),
        ("open", "bet"),
        ("open", "close"),
        ("mid", "bet"),
        ("mid", "close"),
        ("bet", "close"),
    ]

    # Compute movement features
    for t1, t2 in intervals:
        prob_move_col = f"prob_move_{t1}_{t2}"
        abs_move_col = f"abs_prob_move_{t1}_{t2}"
        price_move_col = f"price_move_{t1}_{t2}"
        strength_col = f"prob_move_strength_{t1}_{t2}"
        hours_col = f"hours_{t1}_{t2}"
        velocity_col = f"velocity_{t1}_{t2}"

        df[prob_move_col] = df[f"prob_{t2}"] - df[f"prob_{t1}"]
        df[abs_move_col] = df[prob_move_col].abs()
        df[price_move_col] = df[f"odds_{t2}"] - df[f"odds_{t1}"]
        df[strength_col] = safe_divide(df[prob_move_col], df[f"prob_{t1}"])

        if f"snapshot_time_{t1}" in df.columns and f"snapshot_time_{t2}" in df.columns:
            df[f"snapshot_time_{t1}"] = pd.to_datetime(df[f"snapshot_time_{t1}"], errors="coerce")
            df[f"snapshot_time_{t2}"] = pd.to_datetime(df[f"snapshot_time_{t2}"], errors="coerce")
            df[hours_col] = (df[f"snapshot_time_{t2}"] - df[f"snapshot_time_{t1}"]).dt.total_seconds() / 3600
            df[velocity_col] = safe_divide(df[prob_move_col], df[hours_col])
        else:
            df[hours_col] = np.nan
            df[velocity_col] = np.nan

    # Normalized / robust variants (based on open→close)
    df["prob_move_z"] = df.groupby("market")["prob_move_open_close"].transform(lambda x: zscore(x, nan_policy="omit"))
    df["prob_move_pctile"] = df.groupby("market")["prob_move_open_close"].transform(lambda x: x.rank(pct=True))

    # Consensus metrics
    df["consensus_move"] = df.groupby(["event_id", "market", "player", "direction"])["prob_move_open_close"].transform("median")

    def book_count_same_dir(x):
        return (np.sign(x) == np.sign(np.nanmedian(x))).sum()

    df["book_count_moved"] = df.groupby(["event_id", "market", "player", "direction"])["prob_move_open_close"].transform(book_count_same_dir)

    # Meta features
    df["prob_move_sign_open_close"] = np.sign(df["prob_move_open_close"])
    threshold = df["abs_prob_move_open_close"].quantile(0.9)
    df["abs_prob_move_open_close_top10"] = (df["abs_prob_move_open_close"] >= threshold).astype(int)
    df["book_consensus_strength"] = df["consensus_move"].abs() * df["book_count_moved"]

    # Rolling average (momentum)
    df = df.sort_values(["event_id", "bookmaker"])
    df["prob_move_open_close_rolling_avg"] = (
        df.groupby(["market", "player"])["prob_move_open_close"]
        .transform(lambda x: x.rolling(window=5, min_periods=1).mean())
    )

    return df


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute bookmaker odds movement features across snapshots.")
    parser.add_argument("--open", required=True, help="Path to openers snapshot (CSV/Parquet).")
    parser.add_argument("--mid", required=True, help="Path to midweek snapshot (CSV/Parquet).")
    parser.add_argument("--bet", required=True, help="Path to bet-time snapshot (CSV/Parquet).")
    parser.add_argument("--close", required=True, help="Path to closing snapshot (CSV/Parquet).")
    parser.add_argument("--out", required=True, help="Output path (.parquet preferred).")

    args = parser.parse_args()

    df_open = _read_table(args.open)
    df_mid = _read_table(args.mid)
    df_bet = _read_table(args.bet)
    df_close = _read_table(args.close)

    print("⚙️ Creating odds movement features ...")
    out = create_odds_movement_features(df_open, df_mid, df_bet, df_close)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        out.to_parquet(out_path, index=False)
        print(f"[context] odds_movement_features -> {out_path}")
    except Exception as e:
        warnings.warn(f"Parquet write failed ({e}); writing CSV instead.")
        csv_path = out_path.with_suffix(".csv")
        out.to_csv(csv_path, index=False)
        print(f"[warn] Wrote CSV instead -> {csv_path}")
