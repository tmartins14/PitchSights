# production

Scheduled jobs to ingest data, build features, run markets, select/stake bets, and publish betlists. Also contains monitoring and line tracking.

## Jobs

- `jobs/update_data.py` — ingestion snapshots (fbref, odds, weather, referees)
- `jobs/build_features.py` — materialize match/team/player features
- `jobs/run_totals_walkforward.py` — fit → predict → calibrate → price
- `jobs/run_h2h_walkforward.py`
- `jobs/run_spreads_walkforward.py`
- `jobs/run_props_shots_walkforward.py`
- `jobs/publish_betlist.py` — write tickets to CSV/DB
- `jobs/snapshot_bet.py` — capture lines at bet time (book, odds, line, ts)

## Execution

- `execution/bet_selector.py`
  - `select_bets(df, buffer, min_ev) -> betlist_df`
- `execution/staking/kelly.py`
  - `size_bets_kelly(betlist, bankroll, fraction=0.5) -> betlist`
- `execution/staking/flat.py`
  - `size_bets_flat(betlist, unit=1.0) -> betlist`

## Monitoring

- `monitoring/data_quality.py` — freshness/missingness checks
- `monitoring/performance/track_roi.py` — rolling ROI/EV/CLV by segment
- `monitoring/performance/clv_tracking.py` — open→close line drift
- `alerts/slack_webhook.py` — push anomalies to Slack

## Production schedule (example, UTC)

- Mon 02:00 — `update_data.py` (fbref last round + future fixtures)
- Mon 02:30 — `build_features.py`
- Tue 02:00 — `snapshot_openers` (odds)
- Wed 02:00 — `run_totals_walkforward.py`
- Thu 02:00 — `run_props_shots_walkforward.py`
- Fri 02:00 — `update_data.py` (news refresh: referees, weather)
- Fri 02:30 — `build_features.py`
- Sat 08:00 — `run_totals/h2h/spreads_walkforward.py` (final prices)
- Sat 08:15 — `run_props_shots_walkforward.py` (minutes/news adjusted)
- Sat 08:30 — `publish_betlist.py` (writes CSV/DB)
- Sat 08:31 — `snapshot_bet.py` (store odds at bet time)
- Post‑match — `snapshot_close.py` + `clv_tracking.py`

Adjust times to your leagues/timezone and cron accordingly.

## CLI examples

```bash
python -m production.jobs.update_data --league epl --season 2025
python -m production.jobs.run_totals_walkforward --league epl --asof 2025-08-15
python -m production.jobs.publish_betlist --market props/shots --league epl
```
