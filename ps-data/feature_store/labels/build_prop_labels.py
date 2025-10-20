# feature_store/labels/build_prop_labels.py
from __future__ import annotations

from typing import Iterable
from pathlib import Path
import argparse
import pandas as pd


# ------------------------------
# Internal helper (pure)
# ------------------------------
def _require(df: pd.DataFrame, cols: Iterable[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


# ------------------------------
# Public, pure library functions
# ------------------------------
def build_sot_labels(
    player_processed: pd.DataFrame,
    player_id_col: str = "player_id",
    order_col: str = "gameweek",
    sot_col: str = "sot",
) -> pd.DataFrame:
    """
    Regression-style SOT labels: one row per player-match.
    Returns cols: [player_id_col, order_col, 'y_sot'].

    Keep it minimal; any binarization against a sportsbook line
    should be done in training/serving, not here.
    """
    _require(player_processed, [player_id_col, order_col, sot_col])
    out = player_processed[[player_id_col, order_col]].copy()
    out["y_sot"] = player_processed[sot_col].astype(float)
    return out


def build_shots_labels(
    player_processed: pd.DataFrame,
    player_id_col: str = "player_id",
    order_col: str = "gameweek",
    shots_col: str = "shots",
) -> pd.DataFrame:
    """
    Regression-style SHOTS labels: one row per player-match.
    Returns cols: [player_id_col, order_col, 'y_shots'].
    """
    _require(player_processed, [player_id_col, order_col, shots_col])
    out = player_processed[[player_id_col, order_col]].copy()
    out["y_shots"] = player_processed[shots_col].astype(float)
    return out


# ------------------------------
# Optional CLI for Makefile usage
# ------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build regression-style prop labels: y_sot and y_shots.")
    parser.add_argument("--players", required=True, help="Path to per-player match CSV.")
    parser.add_argument("--out-sot", required=True, help="Output path for SOT labels (.parquet).")
    parser.add_argument("--out-shots", required=True, help="Output path for SHOTS labels (.parquet).")
    # Column mappings (override in Makefile if your schema differs)
    parser.add_argument("--player-id-col", default="player_id")
    parser.add_argument("--order-col", default="gameweek")
    parser.add_argument("--sot-col", default="sot")
    parser.add_argument("--shots-col", default="shots")
    args = parser.parse_args()

    df = pd.read_csv(args.players)

    sot_df = build_sot_labels(
        df,
        player_id_col=args.player_id_col,
        order_col=args.order_col,
        sot_col=args.sot_col,
    )
    shots_df = build_shots_labels(
        df,
        player_id_col=args.player_id_col,
        order_col=args.order_col,
        shots_col=args.shots_col,
    )

    Path(args.out_sot).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_shots).parent.mkdir(parents=True, exist_ok=True)
    sot_df.to_parquet(args.out_sot, index=False)
    shots_df.to_parquet(args.out_shots, index=False)

    print(f"[labels] props — SOT -> {args.out_sot}")
    print(f"[labels] props — SHOTS -> {args.out_shots}")
