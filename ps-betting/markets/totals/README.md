# markets/totals

Model & price match total goals. Keep it plain: expected goals per team → distribution → price lines.

## Files

- `features.py`
  - `build_X(match_context, team_feats) -> X_match`
- `model.py`
  - `fit_attack_defense(X_train, y_train) -> model`
  - `predict_lambdas(model, X_test) -> (lambda_home, lambda_away)`
- `price.py`
  - `price_over_under(lambda_home, lambda_away, lines=[2.5,3.0]) -> df (prob_over, prob_under, fair_odds)`
- `eval.py`
  - `evaluate_totals(y_true, p_over) -> {brier, logloss, calib}`
- `pipeline.py`
  - `run_walkforward_totals(config) -> predictions_df`

## Inputs

- `data/features/match_context/*.parquet`
- `data/features/team_features/*.parquet`

## Outputs

- `data/artifacts/predictions/totals/*.parquet`
