import pandas as pd
import numpy as np
from feature_store.expected_pace import make_expected_pace

def test_expected_pace_basic_blend():
    matches = pd.DataFrame({"home_team":["A","B"], "away_team":["B","A"]})
    strength = pd.DataFrame({
        "team":["A","B"],
        "pace_for":[10.0, 14.0],
        "pace_against":[12.0, 16.0],
    })
    out = make_expected_pace(matches, strength)
    assert list(out.columns) == ["exp_pace_home","exp_pace_away","exp_pace_total"]
    assert np.all(np.isfinite(out.to_numpy()))

    # row0: home=A, away=B
    # exp_home = (A.pf + B.pa)/2 = (10 + 16)/2 = 13
    # exp_away = (B.pf + A.pa)/2 = (14 + 12)/2 = 13
    # total = 26
    assert out.iloc[0].to_dict() == {"exp_pace_home":13.0,"exp_pace_away":13.0,"exp_pace_total":26.0}

def test_expected_pace_handles_missing_team_rows():
    matches = pd.DataFrame({"home_team":["A"], "away_team":["X"]})  # X not in strength
    strength = pd.DataFrame({"team":["A"], "pace_for":[10.0], "pace_against":[12.0]})
    out = make_expected_pace(matches, strength)
    # Will produce NaN for away lookups; ensure columns exist and are floats
    assert set(out.columns) == {"exp_pace_home","exp_pace_away","exp_pace_total"}
    assert out.isna().any().any()  # some NaNs expected due to missing X
