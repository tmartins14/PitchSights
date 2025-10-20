# tests

Unit/integration tests. Ensure contracts don’t break.

## Examples

- `tests/common/test_metrics.py` — EV/ROI/CLV math
- `tests/feature_store/test_player_features.py` — required columns exist
- `tests/markets/props/test_pricing.py` — Poisson pricing edges
- `tests/production/test_bet_selector.py` — filter logic + buffers

## How to run

```bash
pytest -q
```
