import requests
import os
from dotenv import load_dotenv

from fetch_historical_match_events import fetch_match_events_for_date, extract_event_metadata

# Load environment variables
load_dotenv()
API_KEY = os.getenv("ODDS_API_KEY")


def fetch_sot_odds_for_event(league, event_id: str, closing_time: str, bet_time: str = None):
    """
    Fetch player shots on target odds for a specific match at:
      - closing_time: match kickoff (required)
      - bet_time: optional, historical odds at time of bet
    
    Returns:
        dict with keys:
            - 'closing': odds data at closing time
            - 'bet': odds data at bet time (if bet_time provided)
    """
    def _query(date_iso):
        url = f"https://api.the-odds-api.com/v4/historical/sports/{league}/events/{event_id}/odds"
        params = {
            "apiKey": API_KEY,
            "regions": "us",
            "markets": "player_shots_on_target",
            "dateFormat": "iso",
            "oddsFormat": "decimal",
            "date": date_iso
        }

        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch odds for event {event_id} on {date_iso}: {response.status_code} {response.text}")
        return response.json()

    results = {
        "closing": _query(closing_time)
    }

    if bet_time:
        results["bet"] = _query(bet_time)

    return results


# Demo block for manual testing
if __name__ == "__main__":
    league = "soccer_epl"
    date = "2024-08-24T00:00:00Z"

    # Step 1: Fetch events
    event_response = fetch_match_events_for_date(league, date)
    events = extract_event_metadata(event_response)

    for event in events:
        event_id = event["event_id"]
        home_team = event["home_team"]
        away_team = event["away_team"]
        commence_time = event["commence_time"]  # This is your "closing time"
        bet_time = "2024-08-23T18:00:00Z"        # Example bet time

        print(f"\n🎯 {home_team} vs {away_team} | Event ID: {event_id}")

        try:
            odds_result = fetch_sot_odds_for_event(league, event_id, commence_time, bet_time=bet_time)

            for label, data in odds_result.items():
                print(f"\n📊 Odds at {label.upper()} ({commence_time if label == 'closing' else bet_time})")

                bookmakers = data.get("data", {}).get("bookmakers", [])
                for bookmaker in bookmakers:
                    print(f"  🏦 {bookmaker['title']}")
                    for market in bookmaker.get("markets", []):
                        if market["key"] != "player_shots_on_target":
                            continue
                        for outcome in market.get("outcomes", []):
                            player = outcome.get("description")
                            direction = outcome.get("name")
                            line = outcome.get("point")
                            price = outcome.get("price")
                            print(f"    {player} - {direction} {line} SOT @ {price}")

        except Exception as e:
            print(f"  ⚠️ Error fetching odds for event {event_id}: {e}")
