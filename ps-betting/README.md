# PitchSights Betting (ps-betting)

## Overview

PitchSights Betting (ps-betting) is a modular sports betting analytics platform designed for predictive modeling and market analysis.  
The goal is to develop a scalable, data-driven betting system capable of analyzing multiple markets (e.g., Head-to-Head, Totals, Spreads, Props) across various leagues.  
The system focuses on statistical modeling, calibration, and automated execution, enabling efficient decision-making and continuous improvement.

## Key Goals

1. **Predictive Accuracy** – Build well-calibrated and high-ranking models for different betting markets.
2. **Automation** – Automate data collection, feature engineering, model training, and bet execution.
3. **Modularity** – Ensure each component (data, features, models, execution) is self-contained and reusable.
4. **Scalability** – Easily add new markets, leagues, and data sources.
5. **Performance Tracking** – Monitor ROI, hit rate, CLV, and other betting KPIs.

## Main Directories

ps-betting/
│
├── archive/ # Archived raw and processed data
├── config/ # Global configuration and environment settings
├── data/ # Data ingestion and storage
│ ├── fbref/ # FBRef historical and match stats
│ ├── odds/ # Historical and real-time odds data
│ ├── news/ # Web-scraped and API-sourced news data
│ └── utils/ # Data cleaning and transformation scripts
│
├── features/ # Feature engineering pipelines
│ ├── shared/ # Features common to all markets
│ ├── h2h/
│ ├── totals/
│ ├── spreads/
│ └── props/
│
├── markets/ # Market-specific modeling and evaluation
│ ├── h2h/
│ ├── totals/
│ ├── spreads/
│ └── props/
│
├── production/ # Production-ready scripts and execution workflows
│ ├── daily_pipeline.py # End-to-end daily process
│ ├── snapshot_bet.py # Captures live odds and lines at bet time
│ ├── schedule.md # Production execution schedule
│ └── utils/ # Shared helpers for production scripts
│
├── strategies/ # Testing and experimental strategies
│ ├── h2h/
│ ├── totals/
│ ├── spreads/
│ └── props/
│
├── PROJECT_STAGES.md # Project roadmap and prompts for each stage
└── README.md # This file

bash
Always show details

Copy

## Setup Instructions

1. **Clone the repository:**

```bash
git clone <repo-url>
cd ps-betting
Install dependencies:

bash
Always show details

Copy
pip install -r requirements.txt
Set up environment variables:

Copy .env.example to .env

Add your API keys, database connection strings, and other secrets

Run initial data collection:

bash
Always show details

Copy
python data/fbref/collect_initial_data.py
python data/odds/collect_initial_odds.py
Run the first pipeline:

bash
Always show details

Copy
python production/daily_pipeline.py
Workflow Summary
Data Ingestion – Collect historical and upcoming match data from FBRef, Odds API, and news sources.

Feature Engineering – Transform raw data into predictive features for each market.

Model Training – Train and tune models for each market, saving results for evaluation.

Calibration & Evaluation – Assess model performance and adjust for calibration/ranking.

Execution – Automatically place or log bets when thresholds are met.

Performance Tracking – Record results, track KPIs, and optimize strategy over time.

For more detailed development stages, see PROJECT_STAGES.md.
```
