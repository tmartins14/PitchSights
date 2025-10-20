# =======================================
# PitchSights Makefile (FBRef + Understat + Odds)
# =======================================
# Usage:
#   make help
#
# Example:
#   make fbref-all LEAGUE=premier_league SEASON=2024-2025 THROTTLE=8
#   make ust-all LEAGUE=la_liga SEASON=2023-2024
#   make odds-season LEAGUE=bundesliga SEASON=2024-2025 ODDS_MARKETS=h2h,spreads
# ---------------------------------------

.DEFAULT_GOAL := help

.PHONY: help default setup-pkg \
        fbref-all fixtures team-stats player-stats normalize \
        ust-all ust-fixtures ust-team ust-players ust-normalize \
        odds-season odds-market-daily \
        clean-latest clean-interim clean-ust \
        team-strength team-style expected-pace possession-split match-labels prop-labels \
        team-rolling opponent-allowed player-matchups

# =======================================
# Global Defaults
# =======================================
PY ?= python
THROTTLE ?= 5

# =======================================
# League & Season Mapping
# =======================================
# User sets these:
LEAGUE ?= premier_league
SEASON ?= 2025-2026

# ----- League mapping -----
ifeq ($(LEAGUE),premier_league)
    LEAGUE_NAME       = Premier League
    LEAGUE_SLUG       = Premier-League
    LEAGUE_NUM        = 9
    SPORT_KEY         = soccer_epl
    UNDERSTAT_LEAGUE  = EPL
    ODDS_LEAGUE_SLUG  = Premier-League
else ifeq ($(LEAGUE),la_liga)
    LEAGUE_NAME       = La Liga
    LEAGUE_SLUG       = La-Liga
    LEAGUE_NUM        = 12
    SPORT_KEY         = soccer_spain_la_liga
    UNDERSTAT_LEAGUE  = LaLiga
    ODDS_LEAGUE_SLUG  = La-Liga
else ifeq ($(LEAGUE),bundesliga)
    LEAGUE_NAME       = Bundesliga
    LEAGUE_SLUG       = Bundesliga
    LEAGUE_NUM        = 20
    SPORT_KEY         = soccer_germany_bundesliga
    UNDERSTAT_LEAGUE  = Bundesliga
    ODDS_LEAGUE_SLUG  = Bundesliga
else ifeq ($(LEAGUE),serie_a)
    LEAGUE_NAME       = Serie A
    LEAGUE_SLUG       = Serie-A
    LEAGUE_NUM        = 11
    SPORT_KEY         = soccer_italy_serie_a
    UNDERSTAT_LEAGUE  = SerieA
    ODDS_LEAGUE_SLUG  = Serie-A
else ifeq ($(LEAGUE),ligue_1)
    LEAGUE_NAME       = Ligue 1
    LEAGUE_SLUG       = Ligue-1
    LEAGUE_NUM        = 13
    SPORT_KEY         = soccer_france_ligue_one
    UNDERSTAT_LEAGUE  = Ligue1
    ODDS_LEAGUE_SLUG  = Ligue-1
else ifeq ($(LEAGUE),primeira_liga)
    LEAGUE_NAME       = Primeira Liga
    LEAGUE_SLUG       = Primeira-Liga
    LEAGUE_NUM        = 32
    SPORT_KEY         = soccer_portugal_primeira_liga
    UNDERSTAT_LEAGUE  = PrimeiraLiga
    ODDS_LEAGUE_SLUG  = Primeira-Liga
else ifeq ($(LEAGUE),eredivisie)
    LEAGUE_NAME       = Eredivisie
    LEAGUE_SLUG       = Eredivisie
    LEAGUE_NUM        = 23
    SPORT_KEY         = soccer_netherlands_eredivisie
    UNDERSTAT_LEAGUE  = Eredivisie
    ODDS_LEAGUE_SLUG  = Eredivisie
else ifeq ($(LEAGUE),championship)
    LEAGUE_NAME       = EFL Championship
    LEAGUE_SLUG       = Championship
    LEAGUE_NUM        = 10
    SPORT_KEY         = soccer_efl_champ
    UNDERSTAT_LEAGUE  = Championship
    ODDS_LEAGUE_SLUG  = Championship
else
    $(error Unknown LEAGUE specified: $(LEAGUE))
endif

# ----- Season mapping -----
ifeq ($(SEASON),2024-2025)
    UNDERSTAT_SEASON  = 2024
    ODDS_SEASON       = 2024-2025
    SEASON_START      = 2024-08-01T00:00:00Z
    SEASON_END        = 2025-06-30T23:59:59Z
else ifeq ($(SEASON),2025-2026)
    UNDERSTAT_SEASON  = 2025
    ODDS_SEASON       = 2025-2026
    SEASON_START      = 2025-08-01T00:00:00Z
    SEASON_END        = 2026-06-30T23:59:59Z
else ifeq ($(SEASON),2023-2024)
    UNDERSTAT_SEASON  = 2023
    ODDS_SEASON       = 2023-2024
    SEASON_START      = 2023-08-01T00:00:00Z
    SEASON_END        = 2024-06-30T23:59:59Z
else
    $(error Unknown SEASON specified: $(SEASON))
endif

ODDS_REGIONS      ?= uk,eu,us
ODDS_BOOKMAKERS   ?=
ODDS_MARKETS      ?= h2h,spreads,totals,player_goal_scorer_anytime,player_shots_on_target

# =======================================
# Data Directories
# =======================================
RAW_FBREF      := data/raw/fbref
INTERIM_FBREF  := data/interim/fbref
RAW_UNDERSTAT  := data/raw/understat
INTERIM_UNDER  := data/interim/understat
RAW_ODDS       := data/raw/odds
DATA_DIR       := data
RAW_DIR        := $(DATA_DIR)/raw
FEATURES_DIR   := $(DATA_DIR)/features

# =======================================
# Help
# =======================================
help:
	@echo ""
	@echo "PitchSights Make targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | sed 's/:.*##/: /' | sort
	@echo ""

default: help

# =======================================
# Setup
# =======================================
setup-pkg: ## Create package __init__.py and ensure data dirs exist
	@touch ingestion/__init__.py \
	       ingestion/fbref/__init__.py \
	       ingestion/understat/__init__.py \
	       ingestion/pipelines/__init__.py \
	       ingestion/utils/__init__.py
	@mkdir -p $(RAW_FBREF) $(INTERIM_FBREF) $(RAW_UNDERSTAT) $(INTERIM_UNDER) $(RAW_ODDS)
	@echo "✅ Package stubs and data dirs ensured."

# =======================================
# FBRef Pipeline
# =======================================
fixtures:
	$(PY) -m ingestion.fbref.fetch_fixtures \
		--league-num $(LEAGUE_NUM) \
		--season $(SEASON) \
		--league-slug $(LEAGUE_SLUG)

team-stats:
	$(PY) -m ingestion.fbref.fetch_team_stats \
		--season $(SEASON) \
		--league-slug $(LEAGUE_SLUG) \
		--throttle $(THROTTLE)

player-stats:
	$(PY) -m ingestion.fbref.fetch_player_stats \
		--season $(SEASON) \
		--league-slug $(LEAGUE_SLUG) \
		--throttle $(THROTTLE)

normalize:
	$(PY) -m ingestion.fbref.normalize

fbref-all: fixtures team-stats player-stats normalize
	@echo "✅ FBRef pipeline complete."

# =======================================
# Understat Pipeline
# =======================================
ust-fixtures:
	$(PY) -m ingestion.understat.fetch_fixtures \
		--league $(UNDERSTAT_LEAGUE) \
		--season $(UNDERSTAT_SEASON)

ust-team:
	$(PY) -m ingestion.understat.fetch_team_stats \
		--league $(UNDERSTAT_LEAGUE) \
		--season $(UNDERSTAT_SEASON)

ust-players:
	$(PY) -m ingestion.understat.fetch_player_stats \
		--league $(UNDERSTAT_LEAGUE) \
		--season $(UNDERSTAT_SEASON)

ust-normalize:
	$(PY) -m ingestion.understat.normalize \
		--league $(UNDERSTAT_LEAGUE) \
		--season $(UNDERSTAT_SEASON)

ust-all: ust-fixtures ust-team ust-players ust-normalize
	@echo "✅ Understat pipeline complete."

# =======================================
# Historical Odds
# =======================================

# --snapshots openers,midweek,bet,close,inplay_20min,inplay_halftime,inplay_70min\


FIXTURES_FILE ?= data/raw/fbref/fixtures_$(SEASON)_$(LEAGUE_SLUG).csv

odds-season:
	$(PY) -m ingestion.odds.backfill \
		--sport-key $(SPORT_KEY) \
		--league-slug $(ODDS_LEAGUE_SLUG) \
		--season $(ODDS_SEASON) \
		--fixtures-file "$(FIXTURES_FILE)" \
		--snapshots inplay_20min,inplay_halftime,inplay_70min\
		--markets "$(ODDS_MARKETS)" \
		--regions $(ODDS_REGIONS) \
		$(if $(ODDS_BOOKMAKERS),--bookmakers "$(ODDS_BOOKMAKERS)",) \
		--outdir $(RAW_ODDS) \
		--cache-ttl 0 \
		--log-usage 1

# =======================================
# Clean / Maintenance
# =======================================
clean-latest:
	@rm -f $(RAW_FBREF)/*_latest.csv 2>/dev/null || true
	@rm -f $(RAW_UNDERSTAT)/*_latest.csv 2>/dev/null || true
	@rm -f $(RAW_UNDERSTAT)/*_latest.json 2>/dev/null || true
	@rm -f $(RAW_ODDS)/*_latest.csv 2>/dev/null || true
	@echo "🧹 Removed legacy latest pointer files."

clean-interim:
	@rm -f $(INTERIM_FBREF)/*.csv 2>/dev/null || true
	@rm -f $(INTERIM_UNDER)/*.csv 2>/dev/null || true
	@echo "🧽 Removed interim CSVs."

clean-ust:
	@rm -f $(RAW_UNDERSTAT)/* 2>/dev/null || true
	@rm -f $(INTERIM_UNDER)/* 2>/dev/null || true
	@echo "🧼 Cleaned Understat raw & interim."


# ============================================================
# Feature Store — Makefile (cleaned)
# Targets: team-strength, team-style, expected-pace, possession-split, match-labels, prop-labels
# ============================================================

# -------------------------
# Season / League - controlled at the top
# -------------------------
# SEASON       ?= 2024-2025
# LEAGUE_SLUG  ?= Premier-League

# -------------------------
# Common paths
# -------------------------
DATA_DIR       ?= data
RAW_DIR        ?= $(DATA_DIR)/raw
PROCESSED_DIR  ?= $(DATA_DIR)/processed
FEATURES_DIR   ?= $(DATA_DIR)/features

# -------------------------
# Inputs
# -------------------------
DATA_MATCHES    ?= $(RAW_DIR)/fbref/fixtures_$(SEASON)_$(LEAGUE_SLUG).csv
TEAM_STATS_FILE ?= $(RAW_DIR)/fbref/team_stats_$(SEASON)_$(LEAGUE_SLUG).csv
DATA_PLAYERS    ?= $(RAW_DIR)/fbref/player_stats_$(SEASON)_$(LEAGUE_SLUG).csv

# -------------------------
# Outputs
# -------------------------
OUT_CONTEXT_DIR  ?= $(FEATURES_DIR)/context
OUT_LABELS_DIR   ?= $(FEATURES_DIR)/labels

OUT_TEAM_RATINGS_SNAPSHOTS ?= $(OUT_CONTEXT_DIR)/team_ratings_snapshots_$(SEASON)_$(LEAGUE_SLUG).parquet
OUT_TEAM_STYLE_MATCH       ?= $(OUT_CONTEXT_DIR)/team_style_match_$(SEASON)_$(LEAGUE_SLUG).parquet
OUT_EXPECTED_PACE          ?= $(OUT_CONTEXT_DIR)/expected_pace_$(SEASON)_$(LEAGUE_SLUG).parquet
OUT_POSSESSION_SPLIT       ?= $(OUT_CONTEXT_DIR)/possession_split_$(SEASON)_$(LEAGUE_SLUG).parquet

OUT_LABELS_TOTALS ?= $(OUT_LABELS_DIR)/match_totals_labels_$(SEASON)_$(LEAGUE_SLUG).parquet
OUT_LABELS_H2H    ?= $(OUT_LABELS_DIR)/match_h2h_labels_$(SEASON)_$(LEAGUE_SLUG).parquet
OUT_LABELS_SOT    ?= $(OUT_LABELS_DIR)/prop_sot_labels_$(SEASON)_$(LEAGUE_SLUG).parquet
OUT_LABELS_SHOTS  ?= $(OUT_LABELS_DIR)/prop_shots_labels_$(SEASON)_$(LEAGUE_SLUG).parquet


# -------------------------
# Column names (override as needed)
# -------------------------
DATE_COL       ?= match_date
MATCH_ID_COL   ?= match_id
HOME_TEAM_COL  ?= home_team
AWAY_TEAM_COL  ?= away_team
HOME_GOALS_COL ?= home_score
AWAY_GOALS_COL ?= away_score
HOME_XG_COL    ?= home_xG
AWAY_XG_COL    ?= away_xG

PLAYER_ID_COL  ?= player_id
ORDER_COL      ?= gameweek
SOT_COL        ?= sot
SHOTS_COL      ?= shots

# -------------------------
# Params
# -------------------------
# Team Strength
TEAM_STRENGTH_USE_XG       ?= auto     # auto|true|false
TEAM_STRENGTH_DECAY_HALF   ?= 20
TEAM_STRENGTH_RIDGE        ?= 20.0
TEAM_STRENGTH_ITERS        ?= 24
TEAM_STRENGTH_WINDOW_GAMES ?= 8

# Team Style
TEAM_STYLE_WINDOW_GAMES ?= 8

# Expected Pace
PACE_WINDOW_GAMES     ?= 8
PACE_SOT_WEIGHT       ?= 0.0
PACE_POSSESS_WEIGHT   ?= 0.0
STYLE_FILE            ?= $(OUT_TEAM_STYLE_MATCH)
STYLE_DELTA           ?= 0.3
PACE_CLAMP_MIN        ?=
PACE_CLAMP_MAX        ?=
STYLE_SHOTS_SCALE 	  ?= 2.0

# Possession Split
HOME_ADV ?= 0.01

# Match Labels
TOTALS_LINE ?= 2.5

# -------------------------
# Aggregations
# -------------------------
.PHONY: all context labels
all: context labels
context: team-strength team-style expected-pace possession-split
labels: match-labels prop-labels

# ============================================================
# Targets
# ============================================================

# --- Team Strength
.PHONY: team-strength
team-strength:
	python -m feature_store.match_context.team_strength \
		--results "$(DATA_MATCHES)" \
		--out "$(OUT_CONTEXT_DIR)/team_ratings_$(SEASON)_$(LEAGUE_SLUG).parquet" \
		--out-snapshots "$(OUT_TEAM_RATINGS_SNAPSHOTS)" \
		--window-games $(TEAM_STRENGTH_WINDOW_GAMES) \
		--use-xg "$(TEAM_STRENGTH_USE_XG)" \
		--decay-half-life $(TEAM_STRENGTH_DECAY_HALF) \
		--ridge $(TEAM_STRENGTH_RIDGE) \
		--iters $(TEAM_STRENGTH_ITERS) \
		--date-col "$(DATE_COL)" \
		--home-team-col "$(HOME_TEAM_COL)" \
		--away-team-col "$(AWAY_TEAM_COL)" \
		--home-goals-col "$(HOME_GOALS_COL)" \
		--away-goals-col "$(AWAY_GOALS_COL)" \
		--home-xg-col "$(HOME_XG_COL)" \
		--away-xg-col "$(AWAY_XG_COL)" \
		--match-id-col "$(MATCH_ID_COL)"

# --- Team Style
.PHONY: team-style
team-style:
	python -m feature_store.match_context.team_style \
		--matches "$(DATA_MATCHES)" \
		--team-stats "$(TEAM_STATS_FILE)" \
		--out "$(OUT_TEAM_STYLE_MATCH)" \
		--date-col "$(DATE_COL)" \
		--home-team-col "$(HOME_TEAM_COL)" \
		--away-team-col "$(AWAY_TEAM_COL)" \
		--match-id-col "$(MATCH_ID_COL)" \
		--window-games $(TEAM_STYLE_WINDOW_GAMES)

# --- Expected Pace
.PHONY: expected-pace
expected-pace: team-style
	python -m feature_store.match_context.expected_pace \
		--matches "$(DATA_MATCHES)" \
		--team-stats "$(TEAM_STATS_FILE)" \
		--out "$(OUT_EXPECTED_PACE)" \
		--date-col "$(DATE_COL)" \
		--home-team-col "$(HOME_TEAM_COL)" \
		--away-team-col "$(AWAY_TEAM_COL)" \
		--match-id-col "$(MATCH_ID_COL)" \
		--window-games $(PACE_WINDOW_GAMES) \
		--sot-weight $(PACE_SOT_WEIGHT) \
		--poss-weight $(PACE_POSSESS_WEIGHT) \
		--blend-method mean \
		--style-file "$(STYLE_FILE)" \
		--style-window-games $(PACE_WINDOW_GAMES) \
		--style-delta $(STYLE_DELTA) \
		--style-shots-scale $(STYLE_SHOTS_SCALE) \
		$(if $(PACE_CLAMP_MIN),--clamp-min $(PACE_CLAMP_MIN)) \
		$(if $(PACE_CLAMP_MAX),--clamp-max $(PACE_CLAMP_MAX))

# --- Possession Split
.PHONY: possession-split
possession-split:
	python -m feature_store.match_context.possession_split \
		--matches "$(DATA_MATCHES)" \
		--team-stats "$(TEAM_STATS_FILE)" \
		--out "$(OUT_POSSESSION_SPLIT)" \
		--date-col "$(DATE_COL)" \
		--home-team-col "$(HOME_TEAM_COL)" \
		--away-team-col "$(AWAY_TEAM_COL)" \
		--match-id-col "$(MATCH_ID_COL)" \
		--window-games $(PACE_WINDOW_GAMES) \
		--home-adv $(HOME_ADV)

# --- Match Labels
.PHONY: match-labels
match-labels:
	python -m feature_store.labels.build_match_labels \
		--matches "$(DATA_MATCHES)" \
		--out-totals "$(OUT_LABELS_TOTALS)" \
		--line $(TOTALS_LINE) \
		--out-h2h "$(OUT_LABELS_H2H)" \
		--order-col "$(ORDER_COL)" \
		--home-goals-col "$(HOME_GOALS_COL)" \
		--away-goals-col "$(AWAY_GOALS_COL)"

# --- Prop Labels
.PHONY: prop-labels
prop-labels:
	python -m feature_store.labels.build_prop_labels \
		--players "$(DATA_PLAYERS)" \
		--out-sot "$(OUT_LABELS_SOT)" \
		--out-shots "$(OUT_LABELS_SHOTS)" \
		--player-id-col "$(PLAYER_ID_COL)" \
		--order-col "$(ORDER_COL)" \
		--sot-col "$(SOT_COL)" \
		--shots-col "$(SHOTS_COL)"


# =========================
# Team Features
# =========================

# Outputs
OUT_TEAM_ROLLING_TEAM   ?= $(FEATURES_DIR)/team/rolling_team_stats_$(SEASON)_$(LEAGUE_SLUG).parquet
OUT_OPP_ALLOWED_TEAM    ?= $(FEATURES_DIR)/team/opponent_allowed_team_$(SEASON)_$(LEAGUE_SLUG).parquet
OUT_OPP_ALLOWED_MATCH   ?= $(FEATURES_DIR)/team/opponent_allowed_match_$(SEASON)_$(LEAGUE_SLUG).parquet

# Windows
TEAM_WINDOWS ?= 3,6,10

# --- Rolling Team Stats
.PHONY: team-rolling
team-rolling:
	python -m feature_store.team.rolling_team_stats \
		--matches "$(DATA_MATCHES)" \
		--team-stats "$(TEAM_STATS_FILE)" \
		--out "$(OUT_TEAM_ROLLING_TEAM)" \
		--date-col "$(DATE_COL)" \
		--home-team-col "$(HOME_TEAM_COL)" \
		--away-team-col "$(AWAY_TEAM_COL)" \
		--match-id-col "$(MATCH_ID_COL)" \
		--windows "$(TEAM_WINDOWS)"

# --- Opponent Allowed (team & match-level)
.PHONY: opponent-allowed
opponent-allowed:
	python -m feature_store.team.opponent_allowed \
		--matches "$(DATA_MATCHES)" \
		--team-stats "$(TEAM_STATS_FILE)" \
		--out-team "$(OUT_OPP_ALLOWED_TEAM)" \
		--out-match "$(OUT_OPP_ALLOWED_MATCH)" \
		--date-col "$(DATE_COL)" \
		--home-team-col "$(HOME_TEAM_COL)" \
		--away-team-col "$(AWAY_TEAM_COL)" \
		--match-id-col "$(MATCH_ID_COL)" \
		--windows "$(TEAM_WINDOWS)"

# =========================
# Player – Matchup Features
# =========================
OUT_PLAYER_MATCHUP      ?= $(FEATURES_DIR)/players/matchup_features_$(SEASON)_$(LEAGUE_SLUG).parquet
OUT_OPP_ALLOWED_MATCH   ?= $(FEATURES_DIR)/team/opponent_allowed_match_$(SEASON)_$(LEAGUE_SLUG).parquet  # CSV fallback auto
OUT_PLAYER_USAGE        ?= $(FEATURES_DIR)/players/usage_details_$(SEASON)_$(LEAGUE_SLUG).parquet
OUT_PLAYER_MINUTES      ?= $(FEATURES_DIR)/players/minutes_model_$(SEASON)_$(LEAGUE_SLUG).parquet

PLAYER_MATCHUP_WINDOW         ?= 6
PLAYER_MATCHUP_ALPHA_ALLOWED  ?= 0.50
PLAYER_MATCHUP_CLAMP_MIN      ?=
PLAYER_MATCHUP_CLAMP_MAX      ?=

.PHONY: player-matchups
player-matchups:
	python -m feature_store.player.matchup_features \
		--matches "$(DATA_MATCHES)" \
		--usage "$(OUT_PLAYER_USAGE)" \
		--minutes "$(OUT_PLAYER_MINUTES)" \
		--opp-allowed "$(OUT_OPP_ALLOWED_MATCH)" \
		--expected-pace "$(OUT_EXPECTED_PACE)" \
		--out "$(OUT_PLAYER_MATCHUP)" \
		--match-id-col "$(MATCH_ID_COL)" \
		--home-team-col "$(HOME_TEAM_COL)" \
		--away-team-col "$(AWAY_TEAM_COL)" \
		--match-date-col "$(DATE_COL)" \
		--window $(PLAYER_MATCHUP_WINDOW) \
		--alpha-allowed $(PLAYER_MATCHUP_ALPHA_ALLOWED) \
		$(if $(PLAYER_MATCHUP_CLAMP_MIN),--clamp-min-team-shots $(PLAYER_MATCHUP_CLAMP_MIN)) \
		$(if $(PLAYER_MATCHUP_CLAMP_MAX),--clamp-max-team-shots $(PLAYER_MATCHUP_CLAMP_MAX))


# -------------------------
# Outputs (Odds)
# -------------------------

OUT_CONTEXT_DIR  ?= $(FEATURES_DIR)/odds
OUT_ODDS_FEATURES ?= $(OUT_CONTEXT_DIR)/odds_movement_features_$(SEASON)_$(LEAGUE_SLUG).parquet


# -------------------------
# CLI Command
# -------------------------
odds-features:
	@echo "📈 Generating odds movement features..."
	$(PY) feature_store/market_signals/odds_movement_features.py \
		--open  $(PROCESSED_DIR)/$(LEAGUE)/$(SEASON)/odds_openers.parquet \
		--mid   $(PROCESSED_DIR)/$(LEAGUE)/$(SEASON)/odds_midweek.parquet \
		--bet   $(PROCESSED_DIR)/$(LEAGUE)/$(SEASON)/odds_bet.parquet \
		--close $(PROCESSED_DIR)/$(LEAGUE)/$(SEASON)/odds_close.parquet \
		--out   $(OUT_ODDS_FEATURES)
