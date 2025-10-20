# feature_store/player/usage_details.py
# Python 3.9.6
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Sequence, List, Dict, Optional
import argparse
import numpy as np
import pandas as pd


# ---------- utils ----------
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

def _to_numeric(df: pd.DataFrame, cols: Iterable[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


# ---------- core ----------
def make_player_usage_rolling(
    players: pd.DataFrame,
    *,
    # schema (matches your headers)
    player_col: str = "player",
    team_col: str = "team",
    date_col: str = "match_date",
    match_id_col: str = "match_id",
    minutes_col: str = "minutes",
    # usage candidates (all present in your headers)
    shots_col: str = "shots",
    sot_col: str = "shots_on_target",
    xg_col: str = "xg",
    npxg_col: str = "npxg",
    touches_att_pa_col: str = "touches_att_pen_area",
    # windows
    windows: Sequence[int] = (3, 6, 10),
    preaggregate: bool = True,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Build leak-free rolling usage shares from player logs.

    Per match:
      team_totals = sum over players (shots, SoT, xG, npxG, touches in PA)
      player share = player_value / team_totals

    Then per player: shift(1) → rolling mean over windows.

    Output (one row per player-match):
      player_key, player, team, match_id, match_date,
      <metric>_share_w{N} for metric in {shots, sot, xg, npxg, touches_pa}
    """
    req = [player_col, team_col, date_col, match_id_col, minutes_col]
    for c in req:
        if c not in players.columns:
            raise ValueError(f"Missing required column: {c}")

    df = players.copy()
    _ensure_datetime(df, date_col)
    _to_numeric(df, [minutes_col, shots_col, sot_col, xg_col, npxg_col, touches_att_pa_col])

    # stable player key
    df["_player_key"] = df[player_col].astype(str) + " || " + df[team_col].astype(str)

    # preaggregate to one row per player-match-date
    group_keys = ["_player_key", player_col, team_col, match_id_col, date_col]
    if preaggregate:
        agg_map = {minutes_col: "sum"}
        for c in [shots_col, sot_col, xg_col, npxg_col, touches_att_pa_col]:
            if c in df.columns:
                agg_map[c] = "sum"
        df = df.groupby(group_keys, dropna=False, as_index=False).agg(agg_map)

    df = df.dropna(subset=["_player_key", match_id_col, date_col]).copy()
    df = df.sort_values(["_player_key", date_col, match_id_col], kind="mergesort").reset_index(drop=True)

    # team totals per (team, match)
    team_tot = df.groupby([team_col, match_id_col], dropna=False).agg({
        shots_col: "sum",
        sot_col: "sum",
        xg_col: "sum",
        npxg_col: "sum",
        touches_att_pa_col: "sum",
    }).rename(columns={
        shots_col: "team_shots",
        sot_col: "team_sot",
        xg_col: "team_xg",
        npxg_col: "team_npxg",
        touches_att_pa_col: "team_touches_pa",
    }).reset_index()

    df = df.merge(team_tot, on=[team_col, match_id_col], how="left")

    # raw shares
    def share(num, den): return (pd.to_numeric(num, errors="coerce") /
                                 (pd.to_numeric(den, errors="coerce") + 1e-9)).clip(0.0, 1.0)

    df["shots_share"]      = share(df[shots_col], df["team_shots"])
    df["sot_share"]        = share(df[sot_col],   df["team_sot"])
    df["xg_share"]         = share(df[xg_col],    df["team_xg"])
    df["npxg_share"]       = share(df[npxg_col],  df["team_npxg"])
    df["touches_pa_share"] = share(df[touches_att_pa_col], df["team_touches_pa"])

    roll_cols = ["shots_share","sot_share","xg_share","npxg_share","touches_pa_share"]

    # leak-free rolling per player
    def _roll_one(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values([date_col, match_id_col]).copy()
        for c in roll_cols:
            g[c] = g[c].shift(1)
        for w in windows:
            for c in roll_cols:
                g[f"{c}_w{w}"] = g[c].rolling(w, min_periods=1).mean()
        return g

    rolled = df.groupby("_player_key", group_keys=False).apply(_roll_one).reset_index(drop=True)

    keep = ["_player_key", player_col, team_col, match_id_col, date_col]
    keep += [f"{c}_w{w}" for w in windows for c in roll_cols]
    out = rolled[keep].rename(columns={"_player_key": "player_key"})

    if verbose:
        print("[usage_details] windows:", list(windows))
        print("[usage_details] produced shares:", ", ".join(roll_cols))

    return out


# ---------- CLI ----------
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build leak-free rolling player usage shares.")
    p.add_argument("--players", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--windows", default="3,6,10")
    p.add_argument("--no-preaggregate", action="store_true")
    p.add_argument("--player-col", default="player")
    p.add_argument("--team-col", default="team")
    p.add_argument("--date-col", default="match_date")
    p.add_argument("--match-id-col", default="match_id")
    p.add_argument("--minutes-col", default="minutes")
    p.add_argument("--shots-col", default="shots")
    p.add_argument("--sot-col", default="shots_on_target")
    p.add_argument("--xg-col", default="xg")
    p.add_argument("--npxg-col", default="npxg")
    p.add_argument("--touches-pa-col", default="touches_att_pen_area")
    args = p.parse_args()

    players = _read_table(args.players)
    windows = [int(x) for x in str(args.windows).split(",") if str(x).strip()]

    out = make_player_usage_rolling(
        players,
        player_col=args.player_col,
        team_col=args.team_col,
        date_col=args.date_col,
        match_id_col=args.match_id_col,
        minutes_col=args.minutes_col,
        shots_col=args.shots_col,
        sot_col=args.sot_col,
        xg_col=args.xg_col,
        npxg_col=args.npxg_col,
        touches_att_pa_col=args.touches_pa_col,
        windows=windows,
        preaggregate=not bool(args.no_preaggregate),
        verbose=True,
    )

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    try:
        out.to_parquet(args.out, index=False)
        print(f"[player] usage_details -> {args.out}")
    except Exception:
        out.to_csv(Path(args.out).with_suffix(".csv"), index=False)
        print("[warn] parquet failed; wrote CSV instead.")
