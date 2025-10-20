# markets/props/shots

Predict player SHOTS (total) and price Over L.

## Files

- `features.py`
  - `build_player_features(player_rolling, opp_allowed, minutes, matchup, usage_delta) -> X_player`
- `model.py`
  - `fit_rate_model(X_train, y_train) -> model`
  - `predict_lambda(model, X_test, exp_minutes) -> lambda_shots`
- `price.py`
  - `prob_over(lambda_shots, line) -> float`
  - `price_over_lines(lambda_shots, lines) -> df`
- `calibrate.py`
  - `bet_zone_calibrate(prob, y, groups, method='platt') -> np.array`
- `eval.py`
  - `evaluate_props(y_true, p_over) -> {brier, logloss, deciles}`
- `pipeline.py`
  - `run_walkforward_props(config) -> predictions_df`

## Inputs

- `data/features/player_features/*.parquet`
- `data/features/team/opponent_allowed/*.parquet`
