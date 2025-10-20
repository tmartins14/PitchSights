import requests

class OddsAPIClient:
    BASE_URL = 'https://api.the-odds-api.com/v4/sports/'

    def __init__(self, api_key):
        self.api_key = api_key

    def get_sports(self):
        url = self.BASE_URL
        params = {'apiKey': self.api_key}
        return self._make_request(url, params)

    def get_odds(self, sport, regions='uk', markets='h2h', odds_format='decimal', date_format='iso'):
        url = f"{self.BASE_URL}{sport}/odds"
        params = {
            'apiKey': self.api_key,
            'regions': regions,
            'markets': markets,
            'oddsFormat': odds_format,
            'dateFormat': date_format
        }
        return self._make_request(url, params)

    def _make_request(self, url, params):
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API request failed ({response.status_code}): {response.text}")
