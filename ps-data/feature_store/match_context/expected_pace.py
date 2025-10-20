# feature_store/match_context/expected_pace.py
from __future__ import annotations

from pathlib import Path
from typing import List, Optional
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
# Long per-team rows, using matches to supply team/date (via match_id)
# ------------------------------
def _team_long_from_team_stats(
    matches: pd.DataFrame,
    team_stats: pd.DataFrame,
    *,
    date_col: str,
    home_team_col: str,
    away_team_col: str,
    match_id_col: str,
) -> pd.DataFrame:
    """
    Expand team_stats rows (home/away) into per-team rows with shots/SOT/possession,
    joining teams and dates from matches using match_id.
    """
    _require(matches, [match_id_col, date_col, home_team_col, away_team_col])
    _require(team_stats, [match_id_col])

    # Bring identifiers from matches onto team_stats
    id_cols = [match_id_col, date_col, home_team_col, away_team_col]
    ts = team_stats.merge(matches[id_cols].drop_duplicates(), on=match_id_col, how="inner")
    if ts.empty:
        raise ValueError("Join on match_id yielded no rows. Check match_id alignment between matches and team_stats.")

    # Expected stat columns in team_stats
    H_SHOTS = "home_shots"
    A_SHOTS = "away_shots"
    H_SOT   = "home_shots_on_target"
    A_SOT   = "away_shots_on_target"
    H_POSS  = "home_possession"   # percent [0,100]
    A_POSS  = "away_possession"

    have_any = [c for c in (H_SHOTS, A_SHOTS, H_SOT, A_SOT, H_POSS, A_POSS) if c in ts.columns]
    if not have_any:
        raise ValueError("team_stats missing required columns: home/away shots, shots_on_target, possession")

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
# Core: leak-free rolling pace features (team-level)
# ------------------------------
def _make_team_pace_rolling(
    matches: pd.DataFrame,
    team_stats: pd.DataFrame,
    *,
    date_col: str,
    home_team_col: str,
    away_team_col: str,
    match_id_col: str,
    window_games: int,
    sot_weight: float,
    poss_weight: float,
) -> pd.DataFrame:
    """
    Build per-team, per-date rolling pace features (shifted then rolling).
    Pace constructed from shots, optionally blended with SOT and possession.
    All outputs suffixed `_w{N}` where N=window_games.

    pace_for_raw     = shots_for     + sot_weight * sot_for
    pace_against_raw = shots_against + sot_weight * sot_against

    If poss_weight > 0:
      shots_per_poss_for     = shots_for     / (poss_for/100)
      shots_per_poss_against = shots_against / (opp_poss/100), opp_poss = 100 - poss_for

      pace_for     = (1-poss_w)*pace_for_raw     + poss_w*(shots_per_poss_for     * (poss_for/100))
      pace_against = (1-poss_w)*pace_against_raw + poss_w*(shots_per_poss_against * (opp_poss/100))
    else:
      pace_for = pace_for_raw; pace_against = pace_against_raw
    """
    long_df = _team_long_from_team_stats(
        matches, team_stats,
        date_col=date_col,
        home_team_col=home_team_col,
        away_team_col=away_team_col,
        match_id_col=match_id_col,
    )

    poss_frac = (long_df["poss_for"] / 100.0).clip(0.0, 1.0)
    opp_poss_frac = (1.0 - poss_frac).clip(0.0, 1.0)

    pace_for_raw = long_df["shots_for"] + float(sot_weight) * long_df["sot_for"]
    pace_against_raw = long_df["shots_against"] + float(sot_weight) * long_df["sot_against"]

    if poss_weight and float(poss_weight) > 0.0:
        spf = _safe_div(long_df["shots_for"], poss_frac)
        spa = _safe_div(long_df["shots_against"], opp_poss_frac)
        pace_for = (1.0 - float(poss_weight)) * pace_for_raw + float(poss_weight) * (spf * poss_frac)
        pace_against = (1.0 - float(poss_weight)) * pace_against_raw + float(poss_weight) * (spa * opp_poss_frac)
    else:
        pace_for = pace_for_raw
        pace_against = pace_against_raw

    long_df["pace_for"] = pd.to_numeric(pace_for, errors="coerce")
    long_df["pace_against"] = pd.to_numeric(pace_against, errors="coerce")

    # anti-leak: shift(1) then rolling mean
    feat_base = ["pace_for", "pace_against"]

    def _roll_one_team(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        for c in feat_base:
            g[c] = g[c].shift(1)
        for c in feat_base:
            g[f"{c}_w{window_games}"] = g[c].rolling(window_games, min_periods=1).mean()
        return g

    rolled = long_df.groupby("team", group_keys=False).apply(_roll_one_team)
    keep_cols = ["team", "date"] + [f"{c}_w{window_games}" for c in feat_base]
    rolled = rolled[keep_cols].drop_duplicates(subset=["team","date"], keep="last").reset_index(drop=True)
    return rolled


# ------------------------------
# Match-level expected pace (blend)
# ------------------------------
def make_expected_pace_from_team_stats(
    matches: pd.DataFrame,
    team_stats: pd.DataFrame,
    *,
    date_col: str = "match_date",
    home_team_col: str = "home_team",
    away_team_col: str = "away_team",
    match_id_col: str = "match_id",
    window_games: int = 8,
    sot_weight: float = 0.0,
    poss_weight: float = 0.0,
    blend_method: str = "mean",          # "mean" or "geom_mean"
    clamp_total_min: Optional[float] = None,
    clamp_total_max: Optional[float] = None,
    # optional style adjustment
    style_df: Optional[pd.DataFrame] = None,
    style_window_games: Optional[int] = None,  # window used in style_df (for column suffix)
    style_delta: float = 0.0,                  # 0 disables
    style_shots_scale: float = 2.0
) -> pd.DataFrame:
    """
    Compute expected pace per match by blending team rolling pace-for/against.

    Base:
      exp_home ≈ blend( home_pace_for_wN , away_pace_against_wN )
      exp_away ≈ blend( away_pace_for_wN , home_pace_against_wN )
      exp_total = exp_home + exp_away

    Optional style bump (if style_df provided & style_delta != 0):
      compat = 0.5*(h_spf + a_spa) + 0.5*(a_spf + h_spa)
      compat_z = z-score within dataset
      exp_home += (style_delta * compat_z)/2
      exp_away += (style_delta * compat_z)/2
      exp_total = exp_home + exp_away  (then optional clamp)
    """
    _require(matches, [date_col, home_team_col, away_team_col, match_id_col])
    _ensure_datetime(matches, date_col)

    rolled = _make_team_pace_rolling(
        matches, team_stats,
        date_col=date_col,
        home_team_col=home_team_col,
        away_team_col=away_team_col,
        match_id_col=match_id_col,
        window_games=window_games,
        sot_weight=sot_weight,
        poss_weight=poss_weight,
    )

    # Merge onto matches as home/away
    left = rolled.rename(columns={"team": home_team_col, "date": date_col})
    right = rolled.rename(columns={"team": away_team_col, "date": date_col})

    df = matches[[date_col, home_team_col, away_team_col, match_id_col]].copy()
    df = df.merge(left, on=[home_team_col, date_col], how="left", suffixes=("", "_home"))
    df = df.merge(right, on=[away_team_col, date_col], how="left", suffixes=("_home", "_away"))

    base = f"_w{window_games}"
    # Resolve columns
    h_pf = df.get(f"pace_for{base}_home", pd.Series(np.nan, index=df.index))
    h_pa = df.get(f"pace_against{base}_home", pd.Series(np.nan, index=df.index))
    a_pf = df.get(f"pace_for{base}_away", pd.Series(np.nan, index=df.index))
    a_pa = df.get(f"pace_against{base}_away", pd.Series(np.nan, index=df.index))

    # Blends
    if str(blend_method).lower() == "geom_mean":
        def gmean(x, y): return np.sqrt(np.maximum(x, 0.0) * np.maximum(y, 0.0))
        exp_home = gmean(h_pf, a_pa)
        exp_away = gmean(a_pf, h_pa)
    else:
        exp_home = 0.5 * (h_pf + a_pa)
        exp_away = 0.5 * (a_pf + h_pa)

    out = df[[match_id_col]].copy()
    out["exp_pace_home"] = pd.to_numeric(exp_home, errors="coerce")
    out["exp_pace_away"] = pd.to_numeric(exp_away, errors="coerce")
    out["exp_pace_total"] = out["exp_pace_home"] + out["exp_pace_away"]
    # --- optional style adjustment (more impactful) ---
    if style_df is not None and float(style_delta) != 0.0:
        # Determine join keys
        join_keys = []
        if (match_id_col in style_df.columns) and (match_id_col in df.columns):
            join_keys = [match_id_col]
        else:
            try_keys = [date_col, home_team_col, away_team_col]
            if all(k in style_df.columns for k in try_keys):
                join_keys = try_keys

        if not join_keys:
            warnings.warn("style_df provided but no join keys matched; skipping style adjustment.")
        else:
            sw = int(style_window_games) if style_window_games else window_games
            suff = f"_w{sw}"
            cols_needed = [
                f"home_shots_per_poss_for{suff}",
                f"home_shots_per_poss_against{suff}",
                f"away_shots_per_poss_for{suff}",
                f"away_shots_per_poss_against{suff}",
            ]
            if not all(c in style_df.columns for c in cols_needed):
                warnings.warn("style_df missing required per-possession columns; skipping style adjustment.")
            else:
                merged = df[join_keys].merge(style_df[join_keys + cols_needed], on=join_keys, how="left")

                h_spf = pd.to_numeric(merged[f"home_shots_per_poss_for{suff}"], errors="coerce")
                h_spa = pd.to_numeric(merged[f"home_shots_per_poss_against{suff}"], errors="coerce")
                a_spf = pd.to_numeric(merged[f"away_shots_per_poss_for{suff}"], errors="coerce")
                a_spa = pd.to_numeric(merged[f"away_shots_per_poss_against{suff}"], errors="coerce")

                # 1) Build compatibility score
                compat = 0.5 * (h_spf + a_spa) + 0.5 * (a_spf + h_spa)

                # 2) Normalize to z-score (robust std with epsilon), then clip to avoid extreme outliers
                compat_z = (compat - compat.mean(skipna=True)) / (compat.std(skipna=True) + 1e-9)
                compat_z = compat_z.clip(-3.0, 3.0).fillna(0.0)

                # 3) Scale to shots so it actually moves the needle.
                #    style_shots_scale is a NEW CLI arg (default 2.0 shots). Multiply by style_delta (0..1).
                bump = float(style_delta) * float(args.style_shots_scale) * compat_z

                # 4) Apply additively & symmetrically (split the bump)
                out["exp_pace_home"]  = out["exp_pace_home"]  + bump / 2.0
                out["exp_pace_away"]  = out["exp_pace_away"]  + bump / 2.0
                out["exp_pace_total"] = out["exp_pace_home"] + out["exp_pace_away"]

                # Optional: clamp totals to a sensible band after bump
                if args.clamp_min is not None or args.clamp_max is not None:
                    out["exp_pace_total"] = out["exp_pace_total"].clip(
                        lower=args.clamp_min if args.clamp_min is not None else -np.inf,
                        upper=args.clamp_max if args.clamp_max is not None else  np.inf
                    )

                # Debug print so you can see it actually applied
                print(f"[style] bump applied: mean={bump.mean():.3f} shots, "
                    f"std={bump.std():.3f}, nonnull={int(bump.notna().sum())}/{len(bump)}")


    # Optional clamp
    if clamp_total_min is not None or clamp_total_max is not None:
        out["exp_pace_total"] = out["exp_pace_total"].clip(
            lower=clamp_total_min if clamp_total_min is not None else -np.inf,
            upper=clamp_total_max if clamp_total_max is not None else np.inf
        )

    return out


# ------------------------------
# CLI
# ------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute expected pace from team_stats with leak-free rolling features (+optional style).")
    parser.add_argument("--matches", required=True, help="Path to fixtures CSV/Parquet (must include match_id).")
    parser.add_argument("--team-stats", required=True, help="Path to team_stats CSV/Parquet (home/away shots, SOT, possession).")
    parser.add_argument("--out", required=True, help="Output path (.parquet preferred).")

    # Schema
    parser.add_argument("--date-col", default="match_date")
    parser.add_argument("--home-team-col", default="home_team")
    parser.add_argument("--away-team-col", default="away_team")
    parser.add_argument("--match-id-col", default="match_id")

    # Rolling / construction params
    parser.add_argument("--window-games", type=int, default=8)
    parser.add_argument("--sot-weight", type=float, default=0.0, help="Weight for SOT in pace (shots + w*SOT).")
    parser.add_argument("--poss-weight", type=float, default=0.0, help="Blend shots with per-possession rates (0=off, 1=only per-possession).")
    parser.add_argument("--blend-method", type=str, default="mean", choices=["mean", "geom_mean"])

    # Clamp (optional)
    parser.add_argument("--clamp-min", type=float, default=None, help="Minimum total pace (optional).")
    parser.add_argument("--clamp-max", type=float, default=None, help="Maximum total pace (optional).")

    # Optional style adjustment
    parser.add_argument("--style-file", type=str, default=None, help="Path to team_style match-level file (CSV/Parquet).")
    parser.add_argument("--style-window-games", type=int, default=None, help="Window used in style file (defaults to --window-games).")
    parser.add_argument("--style-delta", type=float, default=0.0, help="Scale of style bump in shots (0 disables).")
    parser.add_argument("--style-shots-scale", type=float, default=2.0,
                    help="How many shots a +1.0 compat_z should move totals by before style_delta. Default 2.0.")

    args = parser.parse_args()

    matches    = _read_table(args.matches)
    team_stats = _read_table(args.team_stats)

    style_df = None
    if args.style_file:
        try:
            style_df = _read_table(args.style_file)
        except Exception:
            warnings.warn("Failed to read style_file; continuing without style adjustment.")

    out = make_expected_pace_from_team_stats(
        matches,
        team_stats,
        date_col=args.date_col,
        home_team_col=args.home_team_col,
        away_team_col=args.away_team_col,
        match_id_col=args.match_id_col,
        window_games=int(args.window_games),
        sot_weight=float(args.sot_weight),
        poss_weight=float(args.poss_weight),
        blend_method=args.blend_method,
        clamp_total_min=args.clamp_min,
        clamp_total_max=args.clamp_max,
        style_df=style_df,
        style_window_games=args.style_window_games,
        style_delta=float(args.style_delta),
        style_shots_scale=float(args.style_shots_scale)
    )

    out_path = Path(args.out); out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        out.to_parquet(out_path, index=False)
        print(f"[context] expected_pace -> {out_path}")
    except Exception:
        csv_path = out_path.with_suffix(".csv")
        out.to_csv(csv_path, index=False)
        print(f"[warn] Parquet write failed; wrote CSV instead -> {csv_path}")
