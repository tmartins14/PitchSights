# /strategies/shots-on-target/scripts/api/fetch_historical_match_events.py

import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("ODDS_API_KEY")


def fetch_match_events_for_date(league, date_iso: str):
    """Fetch historical match events for a given league and ISO date string."""
    BASE_URL = f"https://api.the-odds-api.com/v4/historical/sports/{league}/events"

    params = {
        "apiKey": API_KEY,
        "dateFormat": "iso",
        "date": date_iso
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch match events: {response.status_code} {response.text}")
    
    return response.json()


def extract_event_metadata(response_json):
    """Extract event_id and commence_time from API response."""
    return [
        {
            "event_id": event["id"],
            "commence_time": event["commence_time"],
            "home_team": event["home_team"],
            "away_team": event["away_team"]
        }
        for event in response_json.get("data", [])
    ]


# if __name__ == "__main__":
#     date = "2024-08-24T00:00:00Z"
#     league = "soccer_epl"
#     response = fetch_match_events_for_date(league, date)
#     events = extract_event_metadata(response)

#     print(f"\nEvents on {date}:")
#     for event in events:
#         print(f"  - {event['event_id']} | {event['commence_time']} | {event['home_team']} vs {event['away_team']}")
