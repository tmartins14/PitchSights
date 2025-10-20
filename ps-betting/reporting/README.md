# reporting

Dashboards and static reports (e.g., Streamlit or HTML).

## Examples

- `dashboards/roi_over_time.py` — cumulative profit & drawdowns
- `dashboards/calibration_plots.py` — reliability curves
- `dashboards/clv_by_book.py` — market beat rate

## Data contract

Dashboards read only from `data/artifacts/*` and never from raw sources.
