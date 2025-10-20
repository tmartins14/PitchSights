# feature_store/labels/build_match_labels.py
from __future__ import annotations

from pathlib import Path
import argparse
import pandas as pd


# ------------------------------
# Internal helpers (pure)
# ------------------------------
def _require(df: pd.DataFrame, cols):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


# ------------------------------
# Public, pure library functions
# ------------------------------
def build_totals_labels(
    match_processed: pd.DataFrame,
    line: float = 2.5,
    home_goals_col: str = "home_goals",
    away_goals_col: str = "away_goals",
    order_col: str = "gameweek",
) -> pd.DataFrame:
    """
    Create Over/Under labels for the totals market.

    Returns a DataFrame with:
      [order_col, 'total_goals', f'over_{line}', f'under_{line}']

    Notes
    -----
    - 'over' is 1 when total_goals > line
    - 'under' is 1 when total_goals < line
    - pushes (total_goals == line) are 0 in both columns (excluded)
    """
    _require(match_processed, [home_goals_col, away_goals_col, order_col])

    goals = match_processed[home_goals_col].astype(float) + match_processed[away_goals_col].astype(float)
    out = match_processed[[order_col]].copy()

    out["total_goals"] = goals
    over_name = f"over_{line}"
    under_name = f"under_{line}"

    out[over_name] = (goals > line).astype(int)
    out[under_name] = (goals < line).astype(int)
    return out


def build_h2h_labels(
    match_processed: pd.DataFrame,
    home_goals_col: str = "home_goals",
    away_goals_col: str = "away_goals",
    order_col: str = "gameweek",
) -> pd.DataFrame:
    """
    Create 3-way match result labels (home/draw/away).

    Returns a DataFrame with:
      [order_col, 'result', 'y_home', 'y_draw', 'y_away']

    Where:
      - result ∈ {'H','D','A'}
      - y_* are one-hot columns corresponding to result
    """
    _require(match_processed, [home_goals_col, away_goals_col, order_col])

    hg = match_processed[home_goals_col].astype(float)
    ag = match_processed[away_goals_col].astype(float)

    # 'H' if home wins, 'A' if away wins, else 'D'
    res = pd.Series("D", index=match_processed.index)
    res = res.mask(hg > ag, "H").mask(hg < ag, "A")

    out = match_processed[[order_col]].copy()
    out["result"] = res
    out["y_home"] = (res == "H").astype(int)
    out["y_draw"] = (res == "D").astype(int)
    out["y_away"] = (res == "A").astype(int)
    return out


# ------------------------------
# Optional CLI for Makefile usage
# ------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build match labels: totals and 1X2 (home/draw/away).")
    parser.add_argument("--matches", required=True, help="Path to matches CSV.")
    parser.add_argument("--out-totals", required=True, help="Output path for totals labels (.parquet).")
    parser.add_argument("--line", type=float, default=2.5, help="Totals line (e.g., 2.5).")
    parser.add_argument("--out-h2h", required=True, help="Output path for 1X2 labels (.parquet).")
    parser.add_argument("--order-col", default="gameweek")
    parser.add_argument("--home-goals-col", default="home_goals")
    parser.add_argument("--away-goals-col", default="away_goals")
    args = parser.parse_args()

    df = pd.read_csv(args.matches)

    totals = build_totals_labels(
        df,
        line=args.line,
        home_goals_col=args.home_goals_col,
        away_goals_col=args.away_goals_col,
        order_col=args.order_col,
    )
    h2h = build_h2h_labels(
        df,
        home_goals_col=args.home_goals_col,
        away_goals_col=args.away_goals_col,
        order_col=args.order_col,
    )

    Path(args.out_totals).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_h2h).parent.mkdir(parents=True, exist_ok=True)
    totals.to_parquet(args.out_totals, index=False)
    h2h.to_parquet(args.out_h2h, index=False)

    print(f"[labels] totals -> {args.out_totals}")
    print(f"[labels] h2h    -> {args.out_h2h}")
