# /strategies/shots-on-target/scripts/api/fetch_historical_sot_odds.py

import requests
import os
from dotenv import load_dotenv

from fetch_historical_match_events import fetch_match_events_for_date, extract_event_metadata

# Load environment variables
load_dotenv()
API_KEY = os.getenv("ODDS_API_KEY")


def fetch_ags_odds_for_event(league, event_id: str, date_iso: str):
    """Fetch player anytime goalscorer odds for a specific match."""
    BASE_URL_TEMPLATE = f"https://api.the-odds-api.com/v4/historical/sports/{league}/events/{event_id}/odds"
    url = BASE_URL_TEMPLATE.format(event_id=event_id)
    
    params = {
        "apiKey": API_KEY,
        "regions": "us",
        "markets": "player_goal_scorer_anytime",
        "dateFormat": "iso",
        "oddsFormat": "decimal",
        "date": date_iso
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch odds for event {event_id}: {response.status_code} {response.text}")

    return response.json()


if __name__ == "__main__":
    date = "2024-08-24T00:00:00Z"
    league = "soccer_epl"

    # Step 1: Get match events and IDs
    event_response = fetch_match_events_for_date(league, date)
    events = extract_event_metadata(event_response)
    event_lookup = {event["event_id"]: event for event in events}

    # Step 2: Loop over each event and fetch SOT odds
    for event in events:
        event_id = event["event_id"]
        home_team = event["home_team"]
        away_team = event["away_team"]
        match_date = event["commence_time"]  # ISO format

        print(f"\n🎯 {home_team} vs {away_team} | {match_date}")

        try:
            odds_response = fetch_ags_odds_for_event(league, event_id, date)
            bookmakers = odds_response.get("data", {}).get("bookmakers", [])

            if not bookmakers:
                print("  ❌ No AGS odds found.")
                continue

            for bookmaker in bookmakers:
                print(f"  🏦 Bookmaker: {bookmaker['title']}")
                for market in bookmaker.get("markets", []):
                    if market["key"] != "player_goal_scorer_anytime":
                        continue
                    for outcome in market.get("outcomes", []):
                        player = outcome.get("description")
                        line = outcome.get("point")
                        direction = outcome.get("name")  # 'Over' or 'Under'
                        price = outcome.get("price")
                        print(f"    {player} - {direction} {line} AGS @ {price}")

        except Exception as e:
            print(f"  ⚠️ Error fetching odds for {event_id}: {e}")
