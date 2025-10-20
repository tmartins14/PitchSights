import pandas as pd
from feature_store.possession_split import estimate_possession

def test_possession_split_bounds_and_sum():
    home = pd.Series(["A","B","C"])
    away = pd.Series(["B","C","A"])
    ctx = {"possession_skill": {"A": 1.2, "B": 1.0, "C": 0.8}, "home_adv": 0.02}

    out = estimate_possession(home, away, ctx)

    assert list(out.columns) == ["exp_pos_home","exp_pos_away"]
    assert ((out >= 0.0) & (out <= 1.0)).values.all()
    # approx sum to 1 per row
    assert (out["exp_pos_home"] + out["exp_pos_away"]).pipe(lambda s: (s.sub(1.0).abs() < 1e-9).all())

def test_possession_skill_monotonicity():
    # If home skill increases, home possession should not decrease (with fixed away)
    home = pd.Series(["A","A"])
    away = pd.Series(["B","B"])
    ctx1 = {"possession_skill": {"A": 1.0, "B": 1.0}, "home_adv": 0.0}
    ctx2 = {"possession_skill": {"A": 2.0, "B": 1.0}, "home_adv": 0.0}

    p1 = estimate_possession(home, away, ctx1)["exp_pos_home"].iloc[0]
    p2 = estimate_possession(home, away, ctx2)["exp_pos_home"].iloc[0]
    assert p2 > p1

def test_possession_scalar_inputs_supported():
    out = estimate_possession("A", "B", {"possession_skill":{"A":1.5,"B":1.0},"home_adv":0.01})
    assert out.shape == (1,2)
