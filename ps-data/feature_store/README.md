# feature_store

Reusable feature builders. No model training here. Outputs tables under `data/features/`.

## Labels (`labels/`)

### `build_prop_labels.py`

#### `build_sot_labels(player_processed) -> pd.DataFrame`
- **Definition:** Creates player-level **Shots on Target (SOT)** labels for prop betting models.  
- **Data required (minimum):**
  - `player_id` — unique player identifier  
  - `match_id` — unique match identifier  
  - `team, opp_team` — team and opponent  
  - `gameweek | match_date` — temporal key  
  - `sot` — actual shots on target in the match  
  - `minutes` — minutes played  
- **Output columns:**
  - `player_id, match_id` — keys  
  - `label_sot` — integer count of SOT in the match  
  - Optional binary columns: `label_sot_ge_1`, `label_sot_ge_2`, … (did the player clear the prop line?)

#### `build_shots_labels(player_processed) -> pd.DataFrame`
- **Definition:** Creates player-level **Shots** labels for prop betting models.  
- **Data required (minimum):**
  - `player_id, match_id, shots`  
- **Output columns:**
  - `player_id, match_id` — keys  
  - `label_shots` — integer shots in the match  
  - Optional binary columns: `label_shots_ge_1`, `label_shots_ge_2`, …

---

### `build_match_labels.py`

#### `build_totals_labels(match_processed, line=2.5) -> pd.DataFrame`
- **Definition:** Creates match-level **total goals** labels for Over/Under betting markets.  
- **Data required (minimum):**
  - `match_id` — unique match identifier  
  - `home_goals, away_goals` — actual match score  
- **Output columns:**
  - `match_id` — key  
  - `total_goals` — sum of home and away goals  
  - `over_{line}` — binary (1 if total_goals > line, else 0)  
  - `under_{line}` — binary (1 if total_goals ≤ line, else 0)

#### `build_h2h_labels(match_processed) -> pd.DataFrame`
- **Definition:** Creates match-level **1X2 outcome** labels for head-to-head betting markets.  
- **Data required (minimum):**
  - `match_id` — unique match identifier  
  - `home_goals, away_goals` — actual match score  
- **Output columns:**
  - `match_id` — key  
  - `label_1x2` — categorical outcome: `home`, `draw`, or `away`  
  - Optional binary one-hot columns: `is_home_win, is_draw, is_away_win`

---

## Match Context (`match_context/`)

### `expected_pace.py`

#### `make_expected_pace(match_processed, team_strength) -> pd.DataFrame`
- **Definition (Expected Pace):** Predicted **tempo** of a match — how event-dense it will be (passes, carries, shots per 90). Higher pace means more possession changes and higher shot volume.  
- **Data required (minimum):**
  - `match_id, home_team, away_team, date|gameweek`  
  - Team-level features (from FBref):  
    - Possession % (`Poss`)  
    - Passes attempted, progressive pass distance (`PrgDist`)  
    - Long pass share (long passes ÷ total passes)  
    - Pressures, tackles+interceptions  
    - Touches by zone (Def/Mid/Att 3rd)  
  - Optionally: team attack/defense ratings from `team_strength`  
- **Output columns:**
  - `match_id` — key  
  - `expected_pace` — numeric index (e.g. expected total shots pace per 90)  
  - Optional decomposition: `expected_pace_home, expected_pace_away`

---

### `possession_split.py`

#### `estimate_possession(home_team, away_team, ctx) -> pd.DataFrame`
- **Definition (Possession Split):** Predicted **share of possession %** between home and away before a match.  
- **Data required (minimum):**
  - `match_id, home_team, away_team, date|gameweek`  
  - Team-level features (from FBref):  
    - Historical possession % (home/away splits)  
    - Passing profile (long pass share, PrgDist)  
    - Pressures, tackles+interceptions  
    - Touches by zone (Def/Mid/Att 3rd)  
- **Output columns:**
  - `match_id` — key  
  - `poss_home` — predicted home team possession %  
  - `poss_away` — predicted away team possession % (≈ 100 - poss_home)

---

### `team_strength.py`

#### `fit_team_ratings(results) -> ratings`
- **Definition (Team Strength):** Fits a rating system (Elo, Glicko, or Dixon–Coles Poisson) to quantify each team’s attack, defense, and home advantage strength.  
- **Data required (minimum):**
  - `date, home_team, away_team, home_goals, away_goals`  
  - Optional: `home_xg, away_xg` for xG-based versions  
- **Output:**
  - Ratings object with per-team parameters (attack, defense, home field, volatility)

#### `expected_goals(ratings, ctx) -> (lambda_home, lambda_away)`
- **Definition:** Expected **goals scored per team**, using fitted ratings and match context. Used as Poisson means for totals/H2H models.  
- **Data required (minimum):**
  - Ratings object from `fit_team_ratings`  
  - Match context: `home_team, away_team, venue, date, rest_days`  
- **Output:**
  - `(lambda_home, lambda_away)` — floats representing expected goals for home and away


## Team features

- `rolling_team_stats.py`
  - `make_team_rolling(processed_team, windows=[3,6,10]) -> df`
- `opponent_allowed.py`
  - `make_allowed_rolling(processed_team, windows=[3,6,10]) -> df`

## Player features

- `base_player_stats.py`
  - `make_player_rolling(processed_player, windows=[3,6,10]) -> df`
- `minutes_model.py`
  - `expected_minutes(player_history, context) -> float`
- `usage_deltas.py`
  - `usage_delta_last3_vs10(player_stats) -> df`
- `matchup_features.py`
  - `make_matchup(player_stats, opp_allowed) -> df`
