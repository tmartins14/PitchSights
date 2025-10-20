# markets/spreads (Asian Handicap)

Convert expected goals into a goal-difference distribution and price AH lines.

## Files

- `features.py` — reuse totals features
- `model.py`
  - `goal_diff_pmf(lambda_home, lambda_away) -> pd.Series(diff->prob)`
- `price.py`
  - `price_ah(pmf, line) -> {win_prob, push_prob, lose_prob, fair_odds}`
  - `decompose_quarter_line(line) -> [line_a, line_b]`
- `eval.py`
  - `evaluate_cover(y_cover, p_cover) -> metrics`
- `pipeline.py`
  - `run_walkforward_spreads(config) -> predictions_df`

## Notes

- Whole lines must include push handling in EV.
- Quarter lines split stake across adjacent lines.
