---

## `common/README.md`

```md
# common

Pure, stateless utilities used across ingestion, feature building, markets, and production.

## Key modules & functions

- `io.py`
  - `read_parquet(path) -> pd.DataFrame`
  - `write_parquet(df, path, atomic=True) -> None`
  - `load_yaml(path) -> dict`
- `time_utils.py`
  - `infer_gameweek(df, date_col) -> pd.Series`
  - `rolling_windows(dates, window_spec) -> indices`
- `geo.py`
  - `haversine_km(lat1, lon1, lat2, lon2) -> float`
- `validation.py`
  - `assert_schema(df, required_cols: list) -> None`
- `metrics.py`
  - `compute_roi(bets_df) -> float`
  - `brier_score(y, p) -> float`
  - `logloss(y, p) -> float`
  - `decile_table(p, y, mask=None) -> pd.DataFrame`
  - `clv(open_odds, close_odds) -> float`
- `calibration.py`
  - `platt_cv(p, y, groups) -> np.ndarray`
  - `isotonic_cv(p, y, groups) -> np.ndarray`
  - `bet_zone_calibrate(p, y, groups, method='platt') -> np.ndarray`
- `odds_utils.py`
  - `implied_prob_from_decimal(odds) -> float`
  - `ev(prob, odds) -> float`
  - `remove_overround(implied_probs: np.ndarray) -> np.ndarray`
- `walkforward.py`
  - `walk_forward(df, group_col, fit_fn, predict_fn) -> Iterator`
- `sampling.py`
  - `group_kfold(X, y, groups, n_splits) -> folds`
- `transforms.py`
  - `time_decay_weights(dates, half_life_days) -> np.ndarray`
- `persistence.py`
  - `artifact_path(market, league, when) -> Path`

## Notes

- No market-specific logic here; keep it reusable.
```
