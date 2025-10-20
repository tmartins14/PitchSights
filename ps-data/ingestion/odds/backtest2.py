
import sys
import os

# script_dir = os.path.dirname(os.path.abspath(__file__))
script_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(script_dir, "..", "..", ".."))

if project_root not in sys.path:
    sys.path.append(project_root)

from config.setup_paths import *
print(sys.path)
import pandas as pd
from datetime import datetime

from dotenv import load_dotenv

from fetch_odds import fetch_match_events_for_date, extract_event_metadata, fetch_sot_odds_for_event

# from api.fetch_historical_match_events import fetch_match_events_for_date, extract_event_metadata
# from api.fetch_historical_sot_odds import fetch_sot_odds_for_event
load_dotenv()
API_KEY = os.getenv("ODDS_API_KEY")
# INPUTS

# Leagues:
# Premier-League - 9
# La-Liga - 12
# Serie-A - 11
# Ligue-1
# Bundesliga - 20

season = '2024-2025'
league = 'Premier-League'

# API Leagues:
# soccer_epl
# soccer_spain_la_liga
# soccer_italy_serie_a
# soccer_france_ligue_one
# soccer_germany_bundesliga

api_league = 'soccer_epl'
from datetime import datetime

def convert_to_iso(date_str):
    """Convert 'Friday August 16, 2024' to '2024-08-16T00:00:00Z'"""
    dt = datetime.strptime(date_str, "%A %B %d, %Y")
    return dt.strftime("%Y-%m-%dT00:00:00Z")

def normalize_commence_time(commence_time_iso):
    """Extract 'YYYY-MM-DD' from 'YYYY-MM-DDTHH:MM:SSZ'"""
    return datetime.fromisoformat(commence_time_iso.replace("Z", "")).date().isoformat()

from datetime import datetime, timedelta

def subtract_hours_from_iso(iso_datetime_str: str, hours: int) -> str:
    """
    Subtracts a specified number of hours from an ISO 8601 datetime string.

    Args:
        iso_datetime_str (str): The input ISO 8601 datetime string, e.g. '2024-08-17T16:30:00Z'
        hours (int): Number of hours to subtract.

    Returns:
        str: The new ISO 8601 datetime string, e.g. '2024-08-17T12:30:00Z'
    """
    # Parse the input string (strip the 'Z' and treat as UTC)
    dt = datetime.strptime(iso_datetime_str.replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
    
    # Subtract the hours
    new_dt = dt - timedelta(hours=hours)
    
    # Return in the same ISO format with 'Z' suffix
    return new_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


date_str = "Friday August 16, 2024"
iso_date = convert_to_iso(date_str)
iso_date

normalize_commence_time('2025-05-25T18:45:00Z')

subtract_hours_from_iso('2025-05-25T18:45:00Z', 24)

# --- Step 1: Load match dates from CSV and fetch event metadata ---

# Step 1.1 — Load the CSV
csv_path = f"../data/raw/sot_match_stats_{league}_{season}.csv"
df = pd.read_csv(csv_path)

# Step 1.2 — Extract unique match dates
raw_dates = df['match_date'].dropna().unique()

# Step 1.3 — Fetch match events for each date and deduplicate by event_id
event_meta_map = {}  # event_id → metadata (commence_time, home_team, away_team)

for raw_date in raw_dates:
    try:
        iso_date = convert_to_iso(raw_date)
        print(f"\n📅 Fetching match events for: {raw_date} → {iso_date}")

        response = fetch_match_events_for_date(api_league, iso_date)
        events = extract_event_metadata(response)

        if not events:
            print("  ⚠️ No events found.")
            continue

        for event in events:
            event_id = event["event_id"]
            if event_id not in event_meta_map:
                event_meta_map[event_id] = {
                    "commence_time": event["commence_time"],
                    "home_team": event["home_team"],
                    "away_team": event["away_team"]
                }

        print(f"✅ Added {len(events)} events from this date (after deduplication).")

    except Exception as e:
        print(f"❌ Error processing {raw_date}: {e}")

event_meta_map

from datetime import datetime, timedelta

def subtract_hours_from_iso(iso_datetime_str: str, hours: int) -> str:
    dt = datetime.strptime(iso_datetime_str.replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
    new_dt = dt - timedelta(hours=hours)
    return new_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

# --- Step 2: Fetch both closing and bet-time odds per player/line/bookmaker ---

all_odds_rows = []

for event_id, event in event_meta_map.items():
    home_team = event.get("home_team")
    away_team = event.get("away_team")
    commence_time = event.get("commence_time")
    match_date = normalize_commence_time(commence_time)
    
    # Calculate bet time 24 hours before kickoff
    bet_time = subtract_hours_from_iso(commence_time, 24)
    closing_time = subtract_hours_from_iso(commence_time, 1)

    print(f"\n🎯 {home_team} vs {away_team} | {match_date} | Event ID: {event_id}")
    print(f"  🕒 Bet Time: {bet_time} | Kickoff: {commence_time}")

    try:
        odds_result = fetch_sot_odds_for_event(api_league, event_id, closing_time=closing_time, bet_time=bet_time)

        combined_odds = {}

        for timing_label, odds_data in odds_result.items():
            bookmakers = odds_data.get("data", {}).get("bookmakers", [])
            for bookmaker in bookmakers:
                bookmaker_name = bookmaker.get("title")
                for market in bookmaker.get("markets", []):
                    if market.get("key") != "player_shots_on_target":
                        continue
                    for outcome in market.get("outcomes", []):
                        player = outcome.get("description")
                        direction = outcome.get("name")
                        line = outcome.get("point")
                        price = outcome.get("price")

                        key = (player, line, direction, bookmaker_name)

                        if key not in combined_odds:
                            combined_odds[key] = {
                                "match_date": match_date,
                                "event_id": event_id,
                                "home_team": home_team,
                                "away_team": away_team,
                                "bookmaker": bookmaker_name,
                                "player": player,
                                "line": line,
                                "direction": direction,
                                "closing_odds": None,
                                "bet_odds": None
                            }

                        combined_odds[key][f"{timing_label}_odds"] = price

        all_odds_rows.extend(combined_odds.values())

    except Exception as e:
        print(f"  ⚠️ Error fetching odds for event {event_id}: {e}")

subtract_hours_from_iso('2024-08-16T19:00:00Z', 24)
'2024-08-15T19:00:00Z'
fetch_sot_odds_for_event(api_league, 'f7ee35bd5614661f0c83e59885c2a933', subtract_hours_from_iso('2025-05-25T15:00:00Z', 1), subtract_hours_from_iso('2025-05-25T15:00:00Z', 24))

odds_df = pd.DataFrame(all_odds_rows)


 