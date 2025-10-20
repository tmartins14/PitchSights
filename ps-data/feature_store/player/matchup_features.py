# feature_store/player/matchup_features.py
# Python 3.9.6
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Sequence
import numpy as np
import pandas as pd


# -----------------------------
# Robust I/O
# -----------------------------
def robust_read_table(path: str) -> pd.DataFrame:
    """
    Try reading as Parquet first; on failure, try CSV.
    If the input path isn't CSV and Parquet fails, also try <stem>.csv.
    """
    p = Path(str(path).strip().strip("'").strip('"'))
    try:
        return pd.read_parquet(p)
    except Exception:
        # explicit CSV?
        if p.suffix.lower() == ".csv":
            return pd.read_csv(p)
        # fallback to same stem with .csv
        csv_path = p.with_suffix(".csv")
        if csv_path.exists():
            return pd.read_csv(csv_path)
        # final attempt: try reading as CSV anyway (handles no/odd suffix)
        try:
            return pd.read_csv(p)
        except Exception:
            raise FileNotFoundError(f"Could not read '{path}' as Parquet or CSV.")


def robust_write_parquet(df: pd.DataFrame, out_path: str) -> str:
    """
    Attempt to write Parquet; on failure, write CSV with same stem and return that path.
    """
    outp = Path(out_path)
    outp.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(outp, index=False)
        print(f"[player] matchup_features -> {outp}")
        return str(outp)
    except Exception:
        csv_path = outp.with_suffix(".csv")
        df.to_csv(csv_path, index=False)
        print(f"[warn] parquet failed; wrote CSV -> {csv_path}")
        return str(csv_path)


# -----------------------------
# Helpers
# -----------------------------
def _require(df: pd.DataFrame, cols: Sequence[str], name: str) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise ValueError(f"[{name}] Missing required columns: {miss}")

def _ensure_datetime(df: pd.DataFrame, col: str) -> None:
    if col in df.columns and not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col], errors="coerce")


# -----------------------------
# Core
# -----------------------------
def make_matchup_projections(
    *,
    # sources (any CSV/Parquet; function is format-agnostic)
    matches: pd.DataFrame,                # fixtures with match_id + {match_date, home_team, away_team}
    usage_df: pd.DataFrame,               # from usage_details.py (rolling shares), keyed by player_key + match_id
    minutes_df: pd.DataFrame,             # from minutes_model.py (exp minutes), keyed by player_key + match_id
    opp_allowed_match: pd.DataFrame,      # match-level opponent allowed (home_/away_ prefixes), keyed by match_id
    expected_pace_df: pd.DataFrame,       # exp_pace_home/away/total, keyed by match_id
    # schema (we ONLY REQUIRE match_id for merging; date is taken from matches)
    player_col: str = "player",
    team_col: str = "team",
    match_id_col: str = "match_id",
    home_team_col: str = "home_team",
    away_team_col: str = "away_team",
    match_date_col: str = "match_date",
    # usage window and column names
    window: int = 6,
    shots_share_col_tmpl: str = "shots_share_w{w}",
    sot_share_col_tmpl:   str = "sot_share_w{w}",
    xg_share_col_tmpl:    str = "xg_share_w{w}",
    # minutes preference
    minutes_pref: Sequence[str] = ("minutes_w{w}", "exp_minutes_ewma"),
    # opponent-allowed column templates (match-level)
    home_allowed_shots_tmpl: str = "home_opp_allowed_shots_w{w}",
    away_allowed_shots_tmpl: str = "away_opp_allowed_shots_w{w}",
    # expected pace column names (match-level, keyed by match_id only)
    exp_pace_home_col: str = "exp_pace_home",
    exp_pace_away_col: str = "exp_pace_away",
    exp_pace_total_col: str = "exp_pace_total",
    # blend and clamps
    alpha_allowed: float = 0.50,          # weight on opponent-allowed (vs pace)
    clamp_min_team_shots: Optional[float] = None,
    clamp_max_team_shots: Optional[float] = None,
) -> pd.DataFrame:
    """
    Build player-level matchup projections for shots/SoT/xG using match_id as the primary key.

    Steps
    -----
    1) From fixtures (matches): fetch {match_date, home_team, away_team} by match_id
    2) Join usage (shares) + minutes on {player_key, match_id}
    3) Join opponent-allowed and expected pace on {match_id}
    4) Determine side (home/away) by comparing player team vs fixtures' home/away teams
    5) Compute team_expected_shots = (1-alpha)*exp_pace_side + alpha*opp_allowed_shots_side
    6) Project player stats: proj = team_expected_shots * share * (exp_minutes/90)

    Anti-leak is handled upstream by usage/minutes builders (shift→rolling). This function is purely combinatorial.
    """
    # ---------- validate basics
    _require(matches, [match_id_col, match_date_col, home_team_col, away_team_col], "matches")
    _require(usage_df, [match_id_col, "player_key", player_col, team_col], "usage_df")
    _require(minutes_df, [match_id_col, "player_key"], "minutes_df")
    _require(expected_pace_df, [match_id_col, exp_pace_home_col, exp_pace_away_col, exp_pace_total_col],
             "expected_pace_df")

    # ensure datetime on fixtures (for the output)
    _ensure_datetime(matches, match_date_col)

    # ---------- derive dynamic column names for window
    shots_share_col = shots_share_col_tmpl.format(w=window)
    sot_share_col   = sot_share_col_tmpl.format(w=window)
    xg_share_col    = xg_share_col_tmpl.format(w=window)
    _require(usage_df, [shots_share_col, sot_share_col, xg_share_col], "usage_df (shares)")

    # minutes preference: first present among templates
    minutes_pick = None
    for cand in minutes_pref:
        name = cand.format(w=window)
        if name in minutes_df.columns:
            minutes_pick = name
            break
    if minutes_pick is None:
        raise ValueError(f"[minutes_df] None of preferred minute columns found: "
                         f"{[c.format(w=window) for c in minutes_pref]}")

    # opponent allowed columns for this window (may be missing -> create NaNs then fill with pace)
    home_allowed_col = home_allowed_shots_tmpl.format(w=window)
    away_allowed_col = away_allowed_shots_tmpl.format(w=window)
    for c in (home_allowed_col, away_allowed_col):
        if c not in opp_allowed_match.columns:
            opp_allowed_match[c] = np.nan

    # ---------- base: one row per player-match from usage
    base = usage_df.copy()

    # merge minutes on (player_key, match_id) -- don't require date here
    min_keep = [match_id_col, "player_key", minutes_pick]
    starter_cols = [c for c in minutes_df.columns if c.startswith("starter_prob_w")]
    min_keep += starter_cols
    base = base.merge(
        minutes_df[min_keep],
        on=[match_id_col, "player_key"],
        how="left",
        suffixes=("", "_min"),
    ).rename(columns={minutes_pick: "exp_minutes"})

    # merge fixtures on match_id → to get date + sides
    base = base.merge(
        matches[[match_id_col, match_date_col, home_team_col, away_team_col]],
        on=[match_id_col],
        how="left",
        suffixes=('','_drop')
    )
    base["is_home"] = (base[team_col] == base[home_team_col]).astype(int)
    base["is_away"] = (base[team_col] == base[away_team_col]).astype(int)

    # merge opponent-allowed on match_id
    base = base.merge(
        opp_allowed_match[[match_id_col, home_allowed_col, away_allowed_col]],
        on=[match_id_col],
        how="left"
    )

    # merge expected pace on match_id
    base = base.merge(
        expected_pace_df[[match_id_col, exp_pace_home_col, exp_pace_away_col, exp_pace_total_col]],
        on=[match_id_col],
        how="left",
        suffixes=('','_drop')
    )

    # choose side-specific drivers
    base["allowed_shots_side"] = np.where(base["is_home"] == 1, base[home_allowed_col], base[away_allowed_col])
    base["exp_pace_side"]     = np.where(base["is_home"] == 1, base[exp_pace_home_col], base[exp_pace_away_col])

    # impute allowed with pace if missing (soft fallback)
    base["allowed_shots_side"] = base["allowed_shots_side"].fillna(base["exp_pace_side"])

    # blended team shot expectation
    base["team_expected_shots"] = (
        (1.0 - float(alpha_allowed)) * pd.to_numeric(base["exp_pace_side"], errors="coerce")
        + float(alpha_allowed) * pd.to_numeric(base["allowed_shots_side"], errors="coerce")
    )

    # optional clamps
    if clamp_min_team_shots is not None:
        base["team_expected_shots"] = base["team_expected_shots"].clip(lower=float(clamp_min_team_shots))
    if clamp_max_team_shots is not None:
        base["team_expected_shots"] = base["team_expected_shots"].clip(upper=float(clamp_max_team_shots))

    # minutes scaling (>=0)
    base["minutes_scale"] = (pd.to_numeric(base["exp_minutes"], errors="coerce") / 90.0).clip(lower=0.0)

    # pick usage shares
    shots_share = pd.to_numeric(base[shots_share_col], errors="coerce").fillna(0.0)
    sot_share   = pd.to_numeric(base[sot_share_col],   errors="coerce").fillna(0.0)
    xg_share    = pd.to_numeric(base[xg_share_col],    errors="coerce").fillna(0.0)

    team_exp = pd.to_numeric(base["team_expected_shots"], errors="coerce").fillna(0.0)

    # projections (volume × share × minutes scaling)
    base["proj_shots"] = team_exp * shots_share * base["minutes_scale"]
    base["proj_sot"]   = team_exp * sot_share   * base["minutes_scale"]
    # Using team shots as attack-volume proxy for xG; can swap to team exp_goals if you later add it
    base["proj_xg"]    = team_exp * xg_share    * base["minutes_scale"]

    # Final tidy frame
    keep_id = [
        "player_key", player_col, team_col,
        match_id_col, match_date_col, home_team_col, away_team_col, "is_home"
    ]
    keep_drivers = [
        exp_pace_home_col, exp_pace_away_col, exp_pace_total_col,
        home_allowed_col, away_allowed_col,
        "exp_pace_side", "allowed_shots_side", "team_expected_shots",
        "exp_minutes", "minutes_scale",
        shots_share_col, sot_share_col, xg_share_col,
    ]
    keep_preds = ["proj_shots", "proj_sot", "proj_xg"]
    out = base[keep_id + keep_drivers + keep_preds].copy()
    return out


# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build player-level matchup projections (shots/SoT/xG) — match_id keyed.")
    ap.add_argument("--matches", required=True, help="Fixtures CSV/Parquet (must include match_id, match_date, home_team, away_team).")
    ap.add_argument("--usage", required=True, help="Player usage shares (from usage_details.py).")
    ap.add_argument("--minutes", required=True, help="Player minutes model (from minutes_model.py).")
    ap.add_argument("--opp-allowed", required=True, help="Opponent-allowed (match-level). Parquet preferred; CSV fallback supported.")
    ap.add_argument("--expected-pace", required=True, help="Expected pace (match-level). Parquet/CSV.")
    ap.add_argument("--out", required=True, help="Output path (.parquet preferred).")

    # schema (no date arg needed; we derive it from matches)
    ap.add_argument("--player-col", default="player")
    ap.add_argument("--team-col", default="team")
    ap.add_argument("--match-id-col", default="match_id")
    ap.add_argument("--home-team-col", default="home_team")
    ap.add_argument("--away-team-col", default="away_team")
    ap.add_argument("--match-date-col", default="match_date")

    # knobs
    ap.add_argument("--window", type=int, default=6)
    ap.add_argument("--alpha-allowed", type=float, default=0.50)
    ap.add_argument("--clamp-min-team-shots", type=float, default=None)
    ap.add_argument("--clamp-max-team-shots", type=float, default=None)

    args = ap.parse_args()

    matches = robust_read_table(args.matches)
    usage_df = robust_read_table(args.usage)
    minutes_df = robust_read_table(args.minutes)
    opp_allowed_match = robust_read_table(args.opp_allowed)
    expected_pace_df = robust_read_table(args.expected_pace)

    out = make_matchup_projections(
        matches=matches,
        usage_df=usage_df,
        minutes_df=minutes_df,
        opp_allowed_match=opp_allowed_match,
        expected_pace_df=expected_pace_df,
        player_col=args.player_col,
        team_col=args.team_col,
        match_id_col=args.match_id_col,
        home_team_col=args.home_team_col,
        away_team_col=args.away_team_col,
        match_date_col=args.match_date_col,
        window=int(args.window),
        alpha_allowed=float(args.alpha_allowed),
        clamp_min_team_shots=args.clamp_min_team_shots,
        clamp_max_team_shots=args.clamp_max_team_shots,
    )

    robust_write_parquet(out, args.out)
