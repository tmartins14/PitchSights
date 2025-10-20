# feature_store/match_context/team_strength.py
# Python 3.9.6
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple, Optional, Iterable, List

import numpy as np
import pandas as pd
from pathlib import Path
import json
import warnings


# =========================
# Config: numerical guards
# =========================
ETA_CLIP   = 3.0    # clamp linear predictor before exp; exp(6)≈403
HESS_FLOOR = -1e-2  # Hessian must be negative; enforce floor to avoid tiny denom
STEP_CLIP  = 0.2    # max absolute parameter change per update step
LR         = 0.5    # learning-rate multiplier for all parameter updates
MAX_AD_NORM = 3.0   # cap || attach||2 and defense||2


# =========================
# Dataclass
# =========================
@dataclass(frozen=True)
class TeamRatings:
    attack: Dict[str, float]
    defense: Dict[str, float]
    mu: float
    home_adv: float
    meta: Dict[str, object]


# =========================
# Utilities
# =========================
def _require(df: pd.DataFrame, cols: Iterable[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

def _normalize_blanks_to_nan(df: pd.DataFrame, cols: Iterable[str]) -> None:
    repl = {"": np.nan, "-": np.nan, "NA": np.nan, "N/A": np.nan, "nan": np.nan, "None": np.nan}
    for c in cols:
        if c in df.columns and df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip().replace(repl)

def _read_table(path: str) -> pd.DataFrame:
    p = Path(path.strip().strip("'").strip('"'))
    if p.suffix.lower() == ".parquet":
        try:
            return pd.read_parquet(p)
        except Exception:
            return pd.read_csv(p)
    return pd.read_csv(p)

def _ensure_dt(df: pd.DataFrame, col: str) -> None:
    if col in df.columns and not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col], errors="coerce")

def _validate_and_prepare_df(
    df: pd.DataFrame,
    date_col: str,
    team_cols: Tuple[str, str],
    goal_cols: Tuple[str, str],
    xg_cols: Tuple[str, str],
) -> pd.DataFrame:
    d = df.copy()
    _normalize_blanks_to_nan(d, [date_col, *team_cols, *goal_cols, *xg_cols])

    d = d.dropna(subset=[date_col, team_cols[0], team_cols[1]])
    _ensure_dt(d, date_col)
    d = d.dropna(subset=[date_col])

    have_home = d[goal_cols[0]].notna() | ((xg_cols[0] in d.columns) & d[xg_cols[0]].notna())
    have_away = d[goal_cols[1]].notna() | ((xg_cols[1] in d.columns) & d[xg_cols[1]].notna())
    d = d[have_home & have_away]

    d[team_cols[0]] = d[team_cols[0]].astype(str)
    d[team_cols[1]] = d[team_cols[1]].astype(str)
    d = d[d[team_cols[0]] != d[team_cols[1]]]

    d = d.sort_values(date_col, kind="mergesort").reset_index(drop=True)
    return d

def _choose_response_series(
    df: pd.DataFrame,
    prefer_xg: bool,
    home_col_goals: str,
    away_col_goals: str,
    home_col_xg: str,
    away_col_xg: str,
) -> tuple[np.ndarray, np.ndarray, Dict[str, bool]]:
    use_home_xg = prefer_xg and (home_col_xg in df.columns)
    use_away_xg = prefer_xg and (away_col_xg in df.columns)

    y_home = df[home_col_goals].astype(float).copy()
    y_away = df[away_col_goals].astype(float).copy()

    if use_home_xg and df[home_col_xg].notna().any():
        mask = df[home_col_xg].notna()
        y_home.loc[mask] = df.loc[mask, home_col_xg].astype(float)
    if use_away_xg and df[away_col_xg].notna().any():
        mask = df[away_col_xg].notna()
        y_away.loc[mask] = df.loc[mask, away_col_xg].astype(float)

    meta = {
        "prefer_xg": bool(prefer_xg),
        "home_xg_col_used": bool(use_home_xg and df.get(home_col_xg, pd.Series([])).notna().any()),
        "away_xg_col_used": bool(use_away_xg and df.get(away_col_xg, pd.Series([])).notna().any()),
        "fallback_to_goals_rows_home": int((~df.get(home_col_xg, pd.Series([np.nan]*len(df))).notna()).sum()) if use_home_xg else int(len(df)),
        "fallback_to_goals_rows_away": int((~df.get(away_col_xg, pd.Series([np.nan]*len(df))).notna()).sum()) if use_away_xg else int(len(df)),
    }
    return y_home.to_numpy(), y_away.to_numpy(), meta


# =========================
# Team stats → rolling covariates (anti-leak)
# =========================
def _autodetect_team_stats_path(results_path: str) -> Optional[Path]:
    base = Path(results_path).resolve().parent
    cands: List[Path] = []
    cands += list(base.glob("team_stats*.parquet"))
    cands += list(base.glob("team_stats*.csv"))
    return cands[0] if cands else None

def _load_team_stats(team_stats_path: Optional[str]) -> Optional[pd.DataFrame]:
    if not team_stats_path:
        return None
    p = Path(team_stats_path)
    if not p.exists():
        return None
    return _read_table(str(p))

def _build_rolled_covariates(
    results: pd.DataFrame,
    team_stats: pd.DataFrame,
    *,
    date_col: str,
    team_cols: Tuple[str, str],
    match_id_col: Optional[str],
    window_games: int,
) -> pd.DataFrame:
    """
    Attach leak-free rolling team covariates to `results` using `team_stats`.

    Prefer join on `match_id`; else fallback to (date, home_team, away_team).
    Compute per-team, per-date shifted rolling means over last N games for:
      shots_for/against, sot_for/against, poss_for/against.
    """
    df = results.copy()
    _ensure_dt(df, date_col)
    home_col, away_col = team_cols

    H_SHOTS = "home_shots"; A_SHOTS = "away_shots"
    H_SOT   = "home_shots_on_target"; A_SOT = "away_shots_on_target"
    H_POSS  = "home_possession"; A_POSS = "away_possession"

    expect_any = [H_SHOTS, A_SHOTS, H_SOT, A_SOT, H_POSS, A_POSS]
    if not any(c in team_stats.columns for c in expect_any):
        warnings.warn("team_stats missing shot/possession columns; skipping stats covariates.")
        return df

    _ensure_dt(team_stats, date_col)

    can_mid = (match_id_col
               and match_id_col in df.columns
               and match_id_col in team_stats.columns
               and df[match_id_col].notna().any()
               and team_stats[match_id_col].notna().any())

    if can_mid:
        mapping = df[[match_id_col, date_col, home_col, away_col]].drop_duplicates()
        stats = team_stats.merge(mapping, on=match_id_col, how="inner")
    else:
        if not ({date_col, home_col, away_col} <= set(team_stats.columns)):
            warnings.warn("No valid join key between team_stats and results; skipping stats covariates.")
            return df
        stats = team_stats.merge(df[[date_col, home_col, away_col]].drop_duplicates(),
                                 on=[date_col, home_col, away_col], how="inner")

    if stats.empty:
        warnings.warn("Join team_stats↔results yielded no rows; skipping stats covariates.")
        return df

    def g(row, col):  # safe getter
        return row[col] if col in row.index else np.nan

    long_rows = []
    for _, r in stats.iterrows():
        # home team row
        long_rows.append({
            "team": r[home_col], "date": r[date_col],
            "shots_for": g(r, H_SHOTS), "shots_against": g(r, A_SHOTS),
            "sot_for": g(r, H_SOT),     "sot_against": g(r, A_SOT),
            "poss_for": g(r, H_POSS),   "poss_against": g(r, A_POSS),
        })
        # away team row
        long_rows.append({
            "team": r[away_col], "date": r[date_col],
            "shots_for": g(r, A_SHOTS), "shots_against": g(r, H_SHOTS),
            "sot_for": g(r, A_SOT),     "sot_against": g(r, H_SOT),
            "poss_for": g(r, A_POSS),   "poss_against": g(r, H_POSS),
        })

    if not long_rows:
        warnings.warn("No per-team rows constructed from team_stats; skipping stats covariates.")
        return df

    long_df = pd.DataFrame(long_rows).dropna(subset=["team", "date"])
    if long_df.empty:
        warnings.warn("No valid (team,date) rows after team_stats clean; skipping stats covariates.")
        return df

    _ensure_dt(long_df, "date")
    long_df = long_df.sort_values(["team", "date"]).reset_index(drop=True)

    feat_cols = ["shots_for", "shots_against", "sot_for", "sot_against", "poss_for", "poss_against"]
    for c in feat_cols:
        if c in long_df.columns:
            long_df[c] = pd.to_numeric(long_df[c], errors="coerce")

    def _roll_one_team(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        for c in feat_cols:
            if c in g.columns:
                g[c] = g[c].shift(1)  # anti-leak
        for c in feat_cols:
            if c in g.columns:
                g[f"{c}_w{window_games}"] = g[c].rolling(window_games, min_periods=1).mean()
        return g

    rolled = long_df.groupby("team", group_keys=False).apply(_roll_one_team)
    keep_cols = ["team", "date"] + [f"{c}_w{window_games}" for c in feat_cols if f"{c}_w{window_games}" in rolled.columns]
    rolled = rolled[keep_cols].drop_duplicates(subset=["team", "date"], keep="last")

    left = rolled.rename(columns={"team": home_col, "date": date_col})
    right = rolled.rename(columns={"team": away_col, "date": date_col})

    df = df.merge(left, on=[home_col, date_col], how="left", suffixes=("", "_home"))
    df = df.merge(right, on=[away_col, date_col], how="left", suffixes=("_home", "_away"))

    base = f"_w{window_games}"
    for c in feat_cols:
        ch = f"{c}{base}_home"; ca = f"{c}{base}_away"
        if ch in df.columns: df.rename(columns={ch: f"home_{c}{base}"}, inplace=True)
        if ca in df.columns: df.rename(columns={ca: f"away_{c}{base}"}, inplace=True)

    return df


# =========================
# Core fit with optional covariates (SAFE optimizer)
# =========================
def _fit_poisson_with_covariates(
    df: pd.DataFrame,
    *,
    use_xg_flag: str,
    decay_half_life_games: float,
    ridge_lambda: float,
    iters: int,
    team_cols: Tuple[str,str],
    goal_cols: Tuple[str,str],
    xg_cols: Tuple[str,str],
    covar_cols_home: List[str],
    covar_cols_away: List[str],
) -> TeamRatings:
    """Internal: DC-style Poisson fit + optional covariates, with stable updates."""
    prefer_xg = {"auto": True, "true": True, "false": False}.get(str(use_xg_flag).lower(), True)
    yh, ya, rs_meta = _choose_response_series(
        df, prefer_xg, goal_cols[0], goal_cols[1], xg_cols[0], xg_cols[1]
    )

    home_t = df[team_cols[0]].astype(str)
    away_t = df[team_cols[1]].astype(str)
    teams = pd.Index(sorted(set(home_t).union(set(away_t))))
    T = len(teams)
    ti = {t: i for i, t in enumerate(teams)}

    n = len(df)
    if decay_half_life_games and decay_half_life_games > 0:
        game_index = np.arange(n)
        w = 0.5 ** ((n - 1 - game_index) / float(decay_half_life_games))
    else:
        w = np.ones(n, dtype=float)
    wv = w.astype(float)

    def _build_X(cols: List[str]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        if not cols:
            return np.zeros((n,0), dtype=float), np.zeros(0, dtype=float), np.zeros(0, dtype=float)
        X = df[cols].astype(float).to_numpy()
        m = X.mean(axis=0)
        s = X.std(axis=0)
        s[s == 0] = 1.0
        return (X - m) / s, m, s

    Xh, mh, sh = _build_X(covar_cols_home)
    Xa, ma, sa = _build_X(covar_cols_away)

    # params
    # --- smart init for mu/H from targets
    home_mean = float(np.clip(np.nanmean(yh), 1e-6, None))
    away_mean = float(np.clip(np.nanmean(ya), 1e-6, None))
    mu = float(np.log(away_mean))     # away ≈ exp(mu)
    H  = float(np.log(home_mean) - mu)  # home ≈ exp(mu + H)

    # params
    attack = np.zeros(T, dtype=float)
    defense = np.zeros(T, dtype=float)
    beta_h = np.zeros(Xh.shape[1], dtype=float)
    beta_a = np.zeros(Xa.shape[1], dtype=float)

    ridge_beta = max(1.0, ridge_lambda)

    hi = home_t.map(ti).to_numpy()
    ai = away_t.map(ti).to_numpy()

    def _safe_step(grad, hess):
        den = hess if (hess < HESS_FLOOR) else HESS_FLOOR
        step = LR * (grad / den)
        return float(np.clip(step, -STEP_CLIP, STEP_CLIP))

    for _ in range(max(1, iters)):
        # predictors (clipped) and λ
        eta_h = mu + H + attack[hi] + defense[ai]
        eta_a = mu +     attack[ai] + defense[hi]
        if Xh.shape[1] > 0: eta_h = eta_h + Xh @ beta_h
        if Xa.shape[1] > 0: eta_a = eta_a + Xa @ beta_a

        eta_h = np.clip(eta_h, -ETA_CLIP, ETA_CLIP)
        eta_a = np.clip(eta_a, -ETA_CLIP, ETA_CLIP)
        lam_h = np.exp(eta_h)
        lam_a = np.exp(eta_a)

        # --- update attack & defense ---
        for t in range(T):
            mask_h = (hi == t)
            if mask_h.any():
                grad = np.sum(wv[mask_h] * (yh[mask_h] - lam_h[mask_h]))
                hess = -np.sum(wv[mask_h] * lam_h[mask_h]) - ridge_lambda
                attack[t] -= _safe_step(grad, hess)

            mask_a = (ai == t)
            if mask_a.any():
                grad = np.sum(wv[mask_a] * (ya[mask_a] - lam_a[mask_a]))
                hess = -np.sum(wv[mask_a] * lam_a[mask_a]) - ridge_lambda
                defense[t] -= _safe_step(grad, hess)

        # center to keep identifiability
        attack -= attack.mean()
        defense -= defense.mean()

        # --- NEW: L2 norm projection (stability safeguard) ---
        for vec in (attack, defense):
            nrm = np.linalg.norm(vec)
            if nrm > MAX_AD_NORM:
                vec *= (MAX_AD_NORM / (nrm + 1e-12))

        # recompute with centered params
        eta_h = mu + H + attack[hi] + defense[ai] + (Xh @ beta_h if Xh.shape[1] > 0 else 0.0)
        eta_a = mu     +     attack[ai] + defense[hi] + (Xa @ beta_a if Xa.shape[1] > 0 else 0.0)
        eta_h = np.clip(eta_h, -ETA_CLIP, ETA_CLIP)
        eta_a = np.clip(eta_a, -ETA_CLIP, ETA_CLIP)
        lam_h = np.exp(eta_h)
        lam_a = np.exp(eta_a)

        # --- update mu & H ---
        grad_mu = np.sum(wv * (yh - lam_h)) + np.sum(wv * (ya - lam_a))
        hess_mu = -np.sum(wv * lam_h) - np.sum(wv * lam_a) - ridge_lambda
        mu -= _safe_step(grad_mu, hess_mu)

        grad_H = np.sum(wv * (yh - lam_h))
        hess_H = -np.sum(wv * lam_h) - ridge_lambda
        H -= _safe_step(grad_H, hess_H)

        # --- update beta vectors (diagonal approx, safe step) ---
        if Xh.shape[1] > 0:
            r = (wv * (yh - lam_h))
            grad_bh = Xh.T @ r - ridge_beta * beta_h
            Wlam = (wv * lam_h)
            hdiag = (Xh * Wlam[:, None]).T @ Xh
            hdiag.flat[:: hdiag.shape[0] + 1] += ridge_beta
            step = LR * (grad_bh / (-np.diag(hdiag) + 1e-9))
            beta_h -= np.clip(step, -STEP_CLIP, STEP_CLIP)

        if Xa.shape[1] > 0:
            r = (wv * (ya - lam_a))
            grad_ba = Xa.T @ r - ridge_beta * beta_a
            Wlam = (wv * lam_a)
            hdiag = (Xa * Wlam[:, None]).T @ Xa
            hdiag.flat[:: hdiag.shape[0] + 1] += ridge_beta
            step = LR * (grad_ba / (-np.diag(hdiag) + 1e-9))
            beta_a -= np.clip(step, -STEP_CLIP, STEP_CLIP)

    meta = {
        "prefer_xg": bool(prefer_xg),
        "decay_half_life_games": float(decay_half_life_games),
        "ridge_lambda": float(ridge_lambda),
        "iters": int(iters),
        "covariates_home": covar_cols_home,
        "covariates_away": covar_cols_away,
        "eta_clip": ETA_CLIP,
        "step_clip": STEP_CLIP,
        "hess_floor": HESS_FLOOR,
        "lr": LR,
    }

    return TeamRatings(
        attack={t: float(attack[ti[t]]) for t in teams},
        defense={t: float(defense[ti[t]]) for t in teams},
        mu=float(mu),
        home_adv=float(H),
        meta=meta,
    )


# =========================
# Public API (global fit)
# =========================
def fit_team_ratings(
    results: pd.DataFrame,
    use_xg: str = "auto",
    decay_half_life_games: float = 20.0,
    ridge_lambda: float = 10.0,   # ↑ safer default
    team_cols: Tuple[str, str] = ("home_team", "away_team"),
    goal_cols: Tuple[str, str] = ("home_goals", "away_goals"),
    xg_cols: Tuple[str, str] = ("home_xg", "away_xg"),
    date_col: str = "date",
    iters: int = 20,              # ↑ allow more but safe steps
    *,
    covariate_cols_home: Optional[List[str]] = None,
    covariate_cols_away: Optional[List[str]] = None,
) -> TeamRatings:
    _require(results, [date_col, *team_cols, *goal_cols])
    df = _validate_and_prepare_df(results, date_col, team_cols, goal_cols, xg_cols)
    covh = covariate_cols_home or []
    cova = covariate_cols_away or []
    return _fit_poisson_with_covariates(
        df,
        use_xg_flag=use_xg,
        decay_half_life_games=decay_half_life_games,
        ridge_lambda=ridge_lambda,
        iters=iters,
        team_cols=team_cols,
        goal_cols=goal_cols,
        xg_cols=xg_cols,
        covar_cols_home=covh,
        covar_cols_away=cova,
    )


def expected_goals(ratings: TeamRatings, home_team: str, away_team: str) -> Tuple[float, float]:
    ah = ratings.attack[home_team]
    da = ratings.defense[away_team]
    aa = ratings.attack[away_team]
    dh = ratings.defense[home_team]
    # For safety, clip η before exp here too (won't change realistic values)
    eta_h = np.clip(ratings.mu + ratings.home_adv + ah + da, -ETA_CLIP, ETA_CLIP)
    eta_a = np.clip(ratings.mu + aa + dh, -ETA_CLIP, ETA_CLIP)
    lam_home = float(np.exp(eta_h))
    lam_away = float(np.exp(eta_a))
    return lam_home, lam_away


# =========================
# Rolling snapshots (per match), w/ optional stats covariates
# =========================
def make_rolling_snapshots(
    results: pd.DataFrame,
    window_games: int = 8,
    use_xg: str = "auto",
    decay_half_life_games: float = 20.0,
    ridge_lambda: float = 10.0,
    team_cols: Tuple[str, str] = ("home_team", "away_team"),
    goal_cols: Tuple[str, str] = ("home_goals", "away_goals"),
    xg_cols: Tuple[str, str] = ("home_xg", "away_xg"),
    date_col: str = "date",
    match_id_col: Optional[str] = "match_id",
    iters: int = 12,
    team_stats: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    if window_games <= 0:
        raise ValueError("window_games must be > 0")

    _require(results, [date_col, *team_cols, *goal_cols])
    df_all = _validate_and_prepare_df(results, date_col, team_cols, goal_cols, xg_cols)
    df_all["_idx"] = np.arange(len(df_all))

    covh, cova = [], []
    if team_stats is not None:
        df_all = _build_rolled_covariates(
            df_all, team_stats,
            date_col=date_col, team_cols=team_cols, match_id_col=match_id_col, window_games=window_games
        )
        base_feats = [
            f"shots_for_w{window_games}", f"shots_against_w{window_games}",
            f"sot_for_w{window_games}",   f"sot_against_w{window_games}",
            f"poss_for_w{window_games}",  f"poss_against_w{window_games}",
        ]
        covh = [f"home_{b}" for b in base_feats if f"home_{b}" in df_all.columns]
        cova = [f"away_{b}" for b in base_feats if f"away_{b}" in df_all.columns]

    team_to_idx = {t: df_all.index[(df_all[team_cols[0]] == t) | (df_all[team_cols[1]] == t)].tolist()
                   for t in sorted(set(df_all[team_cols[0]]).union(set(df_all[team_cols[1]])))}

    out_rows = []
    for i, row in df_all.iterrows():
        h, a = row[team_cols[0]], row[team_cols[1]]
        asof_date = row[date_col]

        def last_n_before(team: str, cutoff_idx: int, n: int) -> list:
            idxs = team_to_idx.get(team, [])
            prev = [j for j in idxs if j < cutoff_idx]
            return prev[-n:]

        train_idx = sorted(set(last_n_before(h, i, window_games) + last_n_before(a, i, window_games)))
        if not train_idx:
            base = {
                "asof_date": asof_date, team_cols[0]: h, team_cols[1]: a,
                "lambda_home": np.nan, "lambda_away": np.nan,
                "attack_home": np.nan, "defense_home": np.nan,
                "attack_away": np.nan, "defense_away": np.nan,
            }
            if match_id_col and match_id_col in df_all.columns:
                base[match_id_col] = row[match_id_col]
            out_rows.append(base)
            continue

        df_train = df_all.loc[train_idx].copy()
        ratings = _fit_poisson_with_covariates(
            df_train,
            use_xg_flag=use_xg,
            decay_half_life_games=decay_half_life_games,
            ridge_lambda=ridge_lambda,
            iters=iters,
            team_cols=team_cols,
            goal_cols=goal_cols,
            xg_cols=xg_cols,
            covar_cols_home=covh,
            covar_cols_away=cova,
        )

        lam_h, lam_a = expected_goals(ratings, h, a)
        out = {
            "asof_date": asof_date,
            team_cols[0]: h, team_cols[1]: a,
            "lambda_home": lam_h, "lambda_away": lam_a,
            "attack_home": ratings.attack.get(h, np.nan),
            "defense_home": ratings.defense.get(h, np.nan),
            "attack_away": ratings.attack.get(a, np.nan),
            "defense_away": ratings.defense.get(a, np.nan),
        }
        if match_id_col and match_id_col in df_all.columns:
            out[match_id_col] = row[match_id_col]
        out_rows.append(out)

    return pd.DataFrame(out_rows).reset_index(drop=True)


# =========================
# CLI
# =========================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fit team ratings (global) + rolling snapshots with optional team stats covariates.")
    parser.add_argument("--results", type=str, required=True, help="Path to matches CSV/Parquet (date, teams, goals, optional xG).")
    parser.add_argument("--team-stats", type=str, required=False, help="Path to team_stats CSV/Parquet (optional).")
    parser.add_argument("--out", type=str, required=False, help="Output path for GLOBAL ratings (.parquet or .json).")
    parser.add_argument("--out-snapshots", type=str, required=False, help="Output path for per-match snapshots (.parquet).")

    parser.add_argument("--window-games", type=int, default=8, help="Last N games per team for snapshots & rolling covars.")
    parser.add_argument("--use-xg", type=str, default="auto", choices=["auto", "true", "false"])
    parser.add_argument("--decay-half-life", type=float, default=20.0)
    parser.add_argument("--ridge", type=float, default=10.0)
    parser.add_argument("--iters", type=int, default=20)

    # Schema
    parser.add_argument("--date-col", type=str, default="match_date")
    parser.add_argument("--home-team-col", type=str, default="home_team")
    parser.add_argument("--away-team-col", type=str, default="away_team")
    parser.add_argument("--home-goals-col", type=str, default="home_score")
    parser.add_argument("--away-goals-col", type=str, default="away_score")
    parser.add_argument("--home-xg-col", type=str, default="home_xG")
    parser.add_argument("--away-xg-col", type=str, default="away_xG")
    parser.add_argument("--match-id-col", type=str, default="match_id")

    args = parser.parse_args()
    results = _read_table(args.results)

    # Read team_stats: prefer explicit arg; else auto-detect next to results
    team_stats = None
    if args.team_stats:
        team_stats = _load_team_stats(args.team_stats)
    if team_stats is None:
        ts_auto = _autodetect_team_stats_path(args.results)
        if ts_auto:
            print(f"[team_strength] Auto-detected team_stats: {ts_auto.name}")
            team_stats = _load_team_stats(str(ts_auto))
        else:
            warnings.warn("No team_stats provided/detected; running ratings-only.")

    # ----- Global ratings (kept ratings-only for interpretability) -----
    ratings = fit_team_ratings(
        results,
        use_xg=args.use_xg,
        decay_half_life_games=args.decay_half_life,
        ridge_lambda=args.ridge,
        team_cols=(args.home_team_col, args.away_team_col),
        goal_cols=(args.home_goals_col, args.away_goals_col),
        xg_cols=(args.home_xg_col, args.away_xg_col),
        date_col=args.date_col,
        iters=args.iters,
        covariate_cols_home=[],
        covariate_cols_away=[],
    )

    print("Fitted team ratings (global):")
    print(f"- mu={ratings.mu:.4f}, home_adv={ratings.home_adv:.4f}")
    print(f"- teams={len(ratings.attack)}; prefer_xg={ratings.meta['prefer_xg']}; decay_half_life={ratings.meta['decay_half_life_games']}; ridge={ratings.meta['ridge_lambda']}")

    if args.out:
        outp = Path(args.out); outp.parent.mkdir(parents=True, exist_ok=True)
        if outp.suffix.lower() == ".parquet":
            pd.DataFrame({
                "team": list(ratings.attack.keys()),
                "attack": list(ratings.attack.values()),
                "defense": [ratings.defense[t] for t in ratings.attack.keys()],
                "mu": ratings.mu,
                "home_adv": ratings.home_adv,
            }).to_parquet(outp, index=False)
            print(f"Saved ratings table to {outp}")
        else:
            payload = {
                "attack": ratings.attack,
                "defense": ratings.defense,
                "mu": ratings.mu,
                "home_adv": ratings.home_adv,
                "meta": ratings.meta,
            }
            outp.write_text(json.dumps(payload, indent=2))
            print(f"Saved ratings JSON to {outp}")

    # ----- Rolling snapshots (optionally with covariates) -----
    if args.window_games and args.out_snapshots:
        snaps = make_rolling_snapshots(
            results,
            window_games=int(args.window_games),
            use_xg=args.use_xg,
            decay_half_life_games=args.decay_half_life,
            ridge_lambda=args.ridge,
            team_cols=(args.home_team_col, args.away_team_col),
            goal_cols=(args.home_goals_col, args.away_goals_col),
            xg_cols=(args.home_xg_col, args.away_xg_col),
            date_col=args.date_col,
            match_id_col=args.match_id_col,
            iters=max(12, args.iters // 2),
            team_stats=team_stats,
        )
        out_snap = Path(args.out_snapshots); out_snap.parent.mkdir(parents=True, exist_ok=True)
        snaps.to_parquet(out_snap, index=False)
        print(f"Saved rolling snapshots to {out_snap}")
