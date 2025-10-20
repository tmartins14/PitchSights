import pandas as pd
import numpy as np
from feature_store.match_context.team_strength import fit_team_ratings, expected_goals

def tiny_results():
    # A beats B at home twice; B draws C; C beats A away (to avoid degenerate)
    return pd.DataFrame({
        "date": pd.to_datetime(["2024-01-01","2024-01-08","2024-01-15","2024-01-22"]),
        "home_team": ["A","A","B","C"],
        "away_team": ["B","B","C","A"],
        "home_goals":[ 2, 3, 1, 2],
        "away_goals":[ 0, 1, 1, 1],
        # optional xG for a mixed availability test
        "home_xg":[ 1.6, np.nan, 1.2, 1.8],
        "away_xg":[ 0.5, 0.9, 0.8, np.nan],
    })

def test_fit_defaults_and_expected_goals_positive():
    ratings = fit_team_ratings(tiny_results(), use_xg="auto", decay_half_life_games=20, ridge_lambda=1.0, iters=6)
    lam_h, lam_a = expected_goals(ratings, "A", "B")
    assert lam_h > 0 and lam_a > 0
    # home advantage should give A some bump at home vs B
    lam_h2, lam_a2 = expected_goals(ratings, "B", "A")
    assert lam_h > lam_h2 or lam_a2 > lam_a  # some asymmetry present

def test_recent_result_influence_with_decay():
    # Two versions: second match heavily favors A; with decay, more recent match should matter more
    df = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-01","2024-02-01"]),
        "home_team": ["A","A"],
        "away_team": ["B","B"],
        "home_goals":[1, 4],
        "away_goals":[0, 0],
    })
    r_low_decay = fit_team_ratings(df, use_xg="false", decay_half_life_games=200, ridge_lambda=1.0, iters=6)
    r_high_decay = fit_team_ratings(df, use_xg="false", decay_half_life_games=1, ridge_lambda=1.0, iters=6)
    lam_old = expected_goals(r_low_decay, "A","B")[0]
    lam_recent = expected_goals(r_high_decay, "A","B")[0]
    assert lam_recent > lam_old  # recent 4-0 should push A's expected goals up

def test_xg_row_level_fallback_auto():
    # With use_xg='auto', rows with xG present should influence more smoothly than goals
    df = tiny_results()
    r = fit_team_ratings(df, use_xg="auto", decay_half_life_games=20, ridge_lambda=1.0, iters=4)
    assert isinstance(r.meta["response_meta"], dict)
    # sanity: keys exist
    assert "home_xg_col_used" in r.meta["response_meta"]
    # expected_goals runs for all seen teams
    _ = expected_goals(r, "A", "C")

def test_identifiability_zero_center():
    r = fit_team_ratings(tiny_results(), use_xg="false", decay_half_life_games=20, ridge_lambda=1.0, iters=6)
    atk_vals = np.array(list(r.attack.values()))
    def_vals = np.array(list(r.defense.values()))
    # zero-sum (approximately, due to numerical)
    assert abs(atk_vals.mean()) < 1e-6
    assert abs(def_vals.mean()) < 1e-6
