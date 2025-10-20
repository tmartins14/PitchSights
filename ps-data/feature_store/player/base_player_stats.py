# feature_store/player/base_player_stats.py
# Python 3.9.6
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Sequence, Dict, List
import argparse
import numpy as np
import pandas as pd


# -----------------------------
# Utils
# -----------------------------
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

def _safe_per90(num: pd.Series, minutes: pd.Series, eps: float = 1e-9) -> pd.Series:
    m = pd.to_numeric(minutes, errors="coerce")
    return pd.to_numeric(num, errors="coerce") / ((m / 90.0) + eps)

def _zscore_clip(s: pd.Series, clip: float = 3.0) -> pd.Series:
    m = s.mean(skipna=True); v = s.std(skipna=True)
    if v == 0 or np.isnan(v):
        return pd.Series(np.zeros(len(s)), index=s.index)
    z = (s - m) / (v + 1e-9)
    return z.clip(-clip, clip)


# -----------------------------
# Core builder
# -----------------------------
def make_player_rolling(
    players: pd.DataFrame,
    *,
    # your schema (defaults match your headers exactly)
    player_col: str = "player",
    team_col: str = "team",
    date_col: str = "match_date",
    match_id_col: str = "match_id",
    position_col: str = "position",
    minutes_col: str = "minutes",
    # stat cols (all present in your sample)
    goals_col: str = "goals",
    shots_col: str = "shots",
    sot_col: str = "shots_on_target",
    xg_col: str = "xg",
    npxg_col: str = "npxg",
    xa_col: str = "xg_assist",
    sca_col: str = "sca",
    gca_col: str = "gca",
    pens_att_col: str = "pens_att",
    miscontrols_col: str = "miscontrols",
    dispossessed_col: str = "dispossessed",
    passes_received_col: str = "passes_received",
    prog_passes_received_col: str = "progressive_passes_received",
    take_ons_col: str = "take_ons",
    take_ons_won_col: str = "take_ons_won",
    take_ons_tackled_col: str = "take_ons_tackled",
    touches_att_3rd_col: str = "touches_att_3rd",
    touches_att_pa_col: str = "touches_att_pen_area",
    # windows / options
    windows: Sequence[int] = (3, 6, 10),
    preaggregate: bool = True,                 # sum if multiple rows per player-match-date
    standardize_by_position: bool = False,     # add *_z per window for per90 metrics
    zclip: float = 3.0,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Leak-free rolling per-90 player features tailored to your FBref-like schema.

    Output (per player-match):
      identifiers: player_key, player, team, match_id, match_date
      for each window w in windows:
        minutes_w{w}, played_w{w}, started_w{w}
        per90: shots_per90_w{w}, sot_per90_w{w}, goals_per90_w{w}, xg_per90_w{w}, npxg_per90_w{w},
               xa_per90_w{w}, sca_per90_w{w}, gca_per90_w{w},
               take_ons_per90_w{w}, take_ons_won_per90_w{w}, take_ons_tackled_per90_w{w},
               touches_att_3rd_per90_w{w}, touches_att_pen_area_per90_w{w},
               prog_passes_received_per90_w{w}, passes_received_per90_w{w}
        rates:  sot_rate_w{w}, take_on_win_rate_w{w}
      (+ optional *_z columns per position for per90 metrics if standardize_by_position=True)
    """
    req = [player_col, team_col, date_col, match_id_col, minutes_col]
    for c in req:
        if c not in players.columns:
            raise ValueError(f"Missing required column: {c}")

    df = players.copy()
    _ensure_datetime(df, date_col)
    _to_numeric(df, [
        minutes_col, goals_col, shots_col, sot_col, xg_col, npxg_col, xa_col, sca_col, gca_col,
        pens_att_col, miscontrols_col, dispossessed_col, passes_received_col, prog_passes_received_col,
        take_ons_col, take_ons_won_col, take_ons_tackled_col, touches_att_3rd_col, touches_att_pa_col
    ])

    # Stable player key (name || team) to avoid name collisions
    df["_player_key"] = df[player_col].astype(str) + " || " + df[team_col].astype(str)

    # Pre-aggregate to one row per player-match-date if needed
    group_keys = ["_player_key", player_col, team_col, match_id_col, date_col]
    if preaggregate:
        agg_cols = [
            minutes_col, goals_col, shots_col, sot_col, xg_col, npxg_col, xa_col, sca_col, gca_col,
            pens_att_col, miscontrols_col, dispossessed_col, passes_received_col, prog_passes_received_col,
            take_ons_col, take_ons_won_col, take_ons_tackled_col, touches_att_3rd_col, touches_att_pa_col
        ]
        agg_map = {c: "sum" for c in agg_cols if c in df.columns}
        # position: take first non-null (role doesn’t “sum”)
        if position_col in df.columns:
            agg_map[position_col] = "first"
        df = df.groupby(group_keys, dropna=False, as_index=False).agg(agg_map)

    # Order & basic derived
    df = df.dropna(subset=["_player_key", match_id_col, date_col]).copy()
    df = df.sort_values(["_player_key", date_col, match_id_col], kind="mergesort").reset_index(drop=True)

    df["minutes"] = pd.to_numeric(df[minutes_col], errors="coerce")
    df["played"]  = (df["minutes"] > 0).astype(float)
    # Heuristic starter flag (no explicit 'started' col provided)
    df["started"] = (df["minutes"] >= 60).astype(float)

    # Per-90 metrics
    def per90(col): return _safe_per90(df[col], df["minutes"]) if col in df.columns else None
    per90_map = {}
    for name, col in [
        ("goals_per90", goals_col),
        ("shots_per90", shots_col),
        ("sot_per90",   sot_col),
        ("xg_per90",    xg_col),
        ("npxg_per90",  npxg_col),
        ("xa_per90",    xa_col),
        ("sca_per90",   sca_col),
        ("gca_per90",   gca_col),
        ("take_ons_per90",          take_ons_col),
        ("take_ons_won_per90",      take_ons_won_col),
        ("take_ons_tackled_per90",  take_ons_tackled_col),
        ("touches_att_3rd_per90",   touches_att_3rd_col),
        ("touches_att_pen_area_per90", touches_att_pa_col),
        ("prog_passes_received_per90", prog_passes_received_col),
        ("passes_received_per90",      passes_received_col),
    ]:
        if col in df.columns:
            df[name] = per90(col)
            per90_map[name] = True

    # Rates
    if (shots_col in df.columns) and (sot_col in df.columns):
        df["sot_rate"] = (pd.to_numeric(df[sot_col], errors="coerce") /
                          (pd.to_numeric(df[shots_col], errors="coerce") + 1e-9)).clip(0.0, 1.0)
    if (take_ons_won_col in df.columns) and (take_ons_col in df.columns):
        df["take_on_win_rate"] = (pd.to_numeric(df[take_ons_won_col], errors="coerce") /
                                  (pd.to_numeric(df[take_ons_col], errors="coerce") + 1e-9)).clip(0.0, 1.0)

    # Columns to roll
    roll_cols: List[str] = ["minutes","played","started"]
    roll_cols += list(per90_map.keys())
    for r in ["sot_rate","take_on_win_rate"]:
        if r in df.columns:
            roll_cols.append(r)

    # Leak-free rolling per player
    def _roll_one(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values([date_col, match_id_col]).copy()
        for c in roll_cols:
            g[c] = g[c].shift(1)  # anti-leak
        for w in windows:
            for c in roll_cols:
                g[f"{c}_w{w}"] = g[c].rolling(w, min_periods=1).mean()
        return g

    rolled = df.groupby("_player_key", group_keys=False).apply(_roll_one).reset_index(drop=True)

    # Keep identifiers and rolled outputs
    keep = ["_player_key", player_col, team_col, match_id_col, date_col]
    keep += [f"{c}_w{w}" for w in windows for c in roll_cols]
    out = rolled[keep].rename(columns={"_player_key":"player_key"})

    # Optional: position-standardized z-scores for per90 metrics
    if standardize_by_position and position_col in df.columns:
        # bring back position per row
        pos = players[[player_col, team_col, match_id_col, date_col, position_col]].copy()
        pos["_player_key"] = players[player_col].astype(str) + " || " + players[team_col].astype(str)
        pos = pos.drop_duplicates(["_player_key", match_id_col, date_col])

        # --- Fix: ensure consistent datetime type ---
        if date_col in out.columns:
            out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
        if date_col in pos.columns:
            pos[date_col] = pd.to_datetime(pos[date_col], errors="coerce")

        out = out.merge(
            pos.rename(columns={"_player_key": "player_key"}),
            on=["player_key", match_id_col, date_col],
            how="left"
        )

        per90_cols = [c for c in roll_cols if c.endswith("_per90")]
        for w in windows:
            cols_w = [f"{c}_w{w}" for c in per90_cols if f"{c}_w{w}" in out.columns]
            for c in cols_w:
                out[f"{c}_z"] = out.groupby(position_col)[c].transform(
                    lambda s: _zscore_clip(s, clip=zclip)
                )

    if verbose:
        print("[base_player_stats] built columns:")
        print("  per90:", ", ".join(sorted([k for k in per90_map.keys()])))
        print("  rates:", ", ".join([r for r in ["sot_rate","take_on_win_rate"] if r in df.columns]))
        print("  windows:", list(windows))

    return out


# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build leak-free rolling per-90 player features (FBref-style schema).")
    p.add_argument("--players", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--windows", default="3,6,10")
    p.add_argument("--no-preaggregate", action="store_true")
    p.add_argument("--standardize-by-position", action="store_true")
    p.add_argument("--zclip", type=float, default=3.0)

    # Optional overrides (defaults already match your headers)
    p.add_argument("--player-col", default="player")
    p.add_argument("--team-col", default="team")
    p.add_argument("--date-col", default="match_date")
    p.add_argument("--match-id-col", default="match_id")
    p.add_argument("--position-col", default="position")
    p.add_argument("--minutes-col", default="minutes")

    args = p.parse_args()

    players = _read_table(args.players)
    windows = [int(x) for x in str(args.windows).split(",") if str(x).strip()]

    out = make_player_rolling(
        players,
        player_col=args.player_col,
        team_col=args.team_col,
        date_col=args.date_col,
        match_id_col=args.match_id_col,
        position_col=args.position_col,
        minutes_col=args.minutes_col,
        windows=windows,
        preaggregate=not bool(args.no_preaggregate),
        standardize_by_position=bool(args.standardize_by_position),
        zclip=float(args.zclip),
        verbose=True,
    )

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    try:
        out.to_parquet(args.out, index=False)
        print(f"[player] base_player_stats -> {args.out}")
    except Exception:
        out.to_csv(Path(args.out).with_suffix(".csv"), index=False)
        print("[warn] parquet failed; wrote CSV instead.")
