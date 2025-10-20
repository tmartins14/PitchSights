# PROJECT_STAGES.md

This document defines the stages of our sports betting project rebuild.  
Each stage is self-contained and includes all necessary context, objectives, scope boundaries, dependencies, and a ready-to-use ChatGPT-5 prompt.

---

## Stage 1: **Project Structure Setup**

**Context:**  
We are pivoting from a purely EPL SoT props model to a more modular framework that can handle multiple markets (H2H, spreads, totals, props). The previous codebase had overlapping functionality and lacked clear separation between production and strategy testing.

**Objective:**  
Create a clean, modular folder structure with documentation for each directory, ensuring it supports multiple markets and separates production execution from experimental strategy testing.

**Scope:**

- Create root folder structure.
- Add `README.md` and `PURPOSE.md` files to key folders.
- No coding logic yet — only structure and documentation.

**Files/Folders to Create or Edit:**

- `/production/`
- `/strategies/` (with subfolders for h2h, spreads, totals, props)
- `/data/`
- `/markets/`
- `/archive/`
- `/config/`
- `/utils/`
- `/tests/`
- `PROJECT_STAGES.md` (this file)

**Dependencies:** None.

**Deliverables:**

- Folder structure created.
- Documentation files explaining each folder’s purpose.

**Prompt to ChatGPT-5:**  
You are ChatGPT-5. We are currently on Stage 1 of our sports betting project rebuild. Up to this point, we have decided to pivot from EPL SoT-only props to a modular multi-market framework.
The goal of this stage is to create a clean, modular folder structure with documentation for each main folder.

We are only working on the following files/folders:

/production/

/strategies/ (h2h, spreads, totals, props subfolders)

/data/

/markets/

/archive/

/config/

/utils/

/tests/

PROJECT_STAGES.md

Please:

Create the folder structure.

Add README.md or PURPOSE.md to each folder explaining its role.

Ensure naming conventions match market names in The Odds API (use h2h for moneyline).

Follow modularity principles so each folder can expand independently.

markdown
Always show details

Copy

**Special Considerations:**

- All temporary or private data goes into `/archive/` (not in GitHub).
- Keep market folder names consistent with The Odds API naming conventions.

---

## Stage 2: **Core Data Collection Framework**

**Context:**  
We now have the project structure in place and need a modular system to collect data from FBRef and The Odds API.

**Objective:**  
Implement reusable functions for collecting, cleaning, and saving historical & live data for all markets.

**Scope:**

- Build modular scraper functions for FBRef.
- Add Odds API fetcher functions.
- Store raw and processed data in `/data/`.

**Files/Folders to Create or Edit:**

- `/data/collect_fbref.py`
- `/data/collect_odds.py`
- `/utils/api_helpers.py`
- `/utils/scraper_helpers.py`

**Dependencies:** Stage 1 completed.

**Deliverables:**

- Functions to fetch FBRef match & player stats.
- Functions to fetch odds data for any market.
- Data saved in `/data/raw/` and `/data/processed/`.

**Prompt to ChatGPT-5:**  
You are ChatGPT-5. We are currently on Stage 2. Stage 1 created our modular folder structure.
The goal of this stage is to build a reusable data collection system for FBRef and The Odds API.

We are only working on these files:

/data/collect_fbref.py

/data/collect_odds.py

/utils/api_helpers.py

/utils/scraper_helpers.py

Tasks:

Create FBRef scraping functions for match, player, and team stats.

Create Odds API fetching functions for all markets (h2h, spreads, totals, props).

Store raw CSV/JSON files in /data/raw/ and processed files in /data/processed/.

Include rate limiting and API key management for The Odds API (20k calls/month).

markdown
Always show details

Copy

**Special Considerations:**

- API call limits must be respected.
- Functions must be callable from both `/production/` and `/strategies/`.

---

## Stage 3: **Feature Engineering**

**Context:**  
With raw data collection in place, we now create modular feature engineering functions that transform raw match/player data into features for modeling.

**Objective:**  
Implement reusable feature engineering pipelines for both main markets and props.

**Scope:**

- Feature engineering for ML models.
- Rolling averages, matchup stats, derived features.

**Files/Folders to Create or Edit:**

- `/features/engineer_features.py`
- `/features/h2h_features.py`
- `/features/props_features.py`

**Dependencies:** Stage 2 data collection complete.

**Deliverables:**

- Functions that take raw data and output ML-ready feature DataFrames.
- Separate feature sets for h2h/spreads/totals vs. props.

**Prompt to ChatGPT-5:**  
You are ChatGPT-5. We are currently on Stage 3. Stage 2 built our data collection system.
The goal of this stage is to create modular feature engineering pipelines for all markets.

We are only working on:

/features/engineer_features.py

/features/h2h_features.py

/features/props_features.py

Tasks:

Create rolling average features.

Create matchup-specific stats.

Implement feature scaling and encoding where needed.

Ensure functions can be imported and run in both testing and production.

markdown
Always show details

Copy

**Special Considerations:**

- Keep functions pure — no side effects or global state changes.
- Minimize feature leakage by shifting features before target.

---

## Stage 4: **Model Development**

**Context:**  
We have clean feature sets for all markets. Now we build MVP models for both main markets and props.

**Objective:**  
Implement modular model training scripts for each market.

**Scope:**

- Train/test split using forward-walk cross-validation.
- Basic model tuning.

**Files/Folders to Create or Edit:**

- `/models/h2h_model.py`
- `/models/props_model.py`

**Dependencies:** Stage 3 features complete.

**Deliverables:**

- Model training and prediction functions.
- Save trained models to `/models/saved/`.
