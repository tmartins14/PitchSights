# ingestion

All scraping/API adapters and normalization. IO only; no feature logic.

## Submodules

- `fbref/`
  - `fetch_fixtures.py` — fixtures/results
    - `fetch_fixtures(league, season) -> pd.DataFrame`
  - `fetch_team_stats.py` — team xG, shots, possession
  - `fetch_player_stats.py` — player per-90 logs
  - `normalize.py` — unify IDs/dtypes; `normalize_ids(df) -> df`
- `odds/`
  - `the_odds_api.py` — rate-limited client + caching
    - `get_odds_snapshot(kind, leagues) -> pd.DataFrame`
  - `snapshot_openers.py`, `snapshot_midweek.py`, `snapshot_close.py`
- `externals/`
  - `weather.py` — OpenWeather wrapper
    - `get_weather(match_dt, venue_latlon) -> dict`
  - `referees.py` — ref assignment & profile
  - `travel.py` — venue coords, `compute_travel(matches) -> pd.DataFrame`
- `pipelines/`
  - `update_daily.py` — end-to-end ingest job

## Notes

- Respect Odds API 20k/month limit with caching & dedup.
- Write raw snapshots with `snapshot_ts` for reproducibility.
