# markets/h2h (moneyline)

Price win/draw/loss using the same expected goals grid from totals.

## Files

- `features.py`
  - `build_X(match_context, team_feats) -> X_match`
- `model.py`
  - `predict_score_grid(lambda_home, lambda_away) -> np.array[score_probs]`
  - `prob_hda(grid) -> (p_home, p_draw, p_away)`
- `price.py`
  - `fair_odds_hda(p_home, p_draw, p_away) -> dict`
- `eval.py`
  - `evaluate_h2h(y_true, p_hda) -> {brier, logloss, calib}`
- `pipeline.py`
  - `run_walkforward_h2h(config) -> predictions_df`

## Inputs / Outputs

Same as totals; consumes lambdas or score grid to stay consistent.
