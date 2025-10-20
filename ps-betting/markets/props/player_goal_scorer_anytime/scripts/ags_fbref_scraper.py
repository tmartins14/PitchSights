import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from collections import defaultdict

import sys
import os

# Get the absolute path to the target file's directory
scraper_dir = os.path.abspath('/Users/tylermartins/Documents/PitchSights/ps-betting/scripts/data_collection/')
sys.path.append(scraper_dir)

# Now import from the file (without the .py extension)
import fbref_scraper as fs

# Leagues:
# Premier-League - 9
# La-Liga - 12
# Serie-A - 11
# Ligue-1 - 13
# Bundesliga - 20

  

# INPUTS
headers = {"User-Agent": "Mozilla/5.0"}
data_path = '../../data/raw'
season = '2024-2025'
league = 'Premier-League'
league_num = '9'
schedule_url = f'https://fbref.com/en/comps/{league_num}/{season}/schedule/{season}-{league}-Scores-and-Fixtures'



# If True, ignore current data and do full webscrape. Else, only scrape URLs where home_score is NaN
full_rerun = False

# schedule_url = 

# ----------------------------------------------------
# Step 2: Get Shots on Target data from the Match Report page
# ----------------------------------------------------

def get_sot_stats(soup):

    # Dictionary to hold merged stats for each player
    merged_player_stats = defaultdict(dict)

    # Get the Match Date
    date_div = soup.find('div', class_ = 'scorebox_meta')

    date_div_tag = date_div.find('a')
    if date_div_tag:
        date = date_div_tag.text.strip()
    else:
        date = date_div.text.strip()



    # Step 1: Find all player_stats divs by id
    player_stats_divs = soup.find_all('div', id=re.compile(r'all_player_stats'))





    table_fields_dict = {
        'summary': [
            'player',
            'position',
            'team',
            'age',
            'minutes',
            'goals',
            'shots',
            'shots_on_target',
            'xg',
            'npxg',
            'xg_assist',
            'sca',
            'gca',
            'pens_att'
        ],
        'possession': [
            'miscontrols',
            'dispossessed',
            'passes_received',
            'progressive_passes_received',
            'take_ons',
            'take_ons_won',
            'take_ons_tackled',
            'touches_att_3rd',
            'touches_att_pen_area'
        ]
    }

    for section, fields in table_fields_dict.items():
        for div in player_stats_divs:
            header = div.find('h2')
            if not header:
                continue
            raw_team_name = header.text.strip()
            team_name = raw_team_name.replace(' Player Stats', '')

            tables = div.find_all('table', id=re.compile(section))
            for table in tables:
                table_body = table.find('tbody')
                if not table_body:
                    continue

                for row in table_body.find_all('tr'):
                    player_th = row.find('th', {'data-stat': 'player'})
                    if not player_th:
                        continue

                    player_name_tag = player_th.find('a')
                    if player_name_tag:
                        player_name = player_name_tag.text.strip()
                    else:
                        player_name = player_th.text.strip()

                    player_key = (player_name, team_name)
                    merged_player_stats[player_key]['match_date'] = date
                    merged_player_stats[player_key]['player'] = player_name
                    merged_player_stats[player_key]['team'] = team_name

                    for field in fields:
                        if field == 'player':
                            continue
                        cell = row.find('td', {'data-stat': field})
                        if cell:
                            merged_player_stats[player_key][field] = cell.text.strip()

    # Convert to a list of dictionaries
    merged_player_list = list(merged_player_stats.values())

    return merged_player_list



# MAIN EXECUTION
print(f'League: {league} - Season: {season} - URL: {schedule_url}')
basic_match_data = fs.get_basic_match_data(schedule_url)

print('Number of Matches: ',len(basic_match_data))

for match in basic_match_data:

    match_url = match['match_url']
    print(f'{(basic_match_data.index(match)+1)}/{len(basic_match_data)}: ',match_url)
    
    if match_url is not None and match['home_score'] is not None:
        response = fs.fetch_url_with_backoff(match_url, max_attempts=12)
        if not response:
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        # Optional small delay to avoid spamming the server
        time.sleep(5)

    else:
        print(f'Matched not Scraped: {match}')
        continue



    # Team-Level Match Stats
    match_details = fs.get_match_details(soup)
    

    combined_match_details = {**basic_match_data[basic_match_data.index(match)], **match_details}
    df_teams = pd.DataFrame([combined_match_details])

    home_df = df_teams[[col for col in df_teams.columns if col != 'match_url']]
    away_df = df_teams[[col for col in df_teams.columns if col != 'match_url']]

    # Rename for merging
    home_df = home_df.rename(columns={'home_team': 'team', 'away_team':'opp'})

    away_df = away_df.rename(columns={'away_team': 'team','home_team':'opp'})

    away_df = away_df.rename(columns=lambda col: col.replace('away_', 'team_') if col.startswith('away_') else (
                                col.replace('home_', 'opp_') if col.startswith('home_') else col))

    home_df = home_df.rename(columns=lambda col: col.replace('home_', 'team_') if col.startswith('home_') else (
                                col.replace('away_', 'opp_') if col.startswith('away_') else col))



    match_df = pd.concat([home_df, away_df])
    sot_match_df = match_df[['match_date','team','team_shots', 'team_shots_on_target','team_xG']]

    # Player-Level Match Stats
    sot_data = get_sot_stats(soup)

    sot_df = pd.DataFrame(sot_data)
    sot_df.columns

    # Merge each side
    total_sot_df = sot_df.merge(sot_match_df, on=['match_date', 'team'], how='left')

    if basic_match_data.index(match) == 0:
        sot_historical_player_stats = total_sot_df
        sot_historical_match_stats = match_df
    else:
        sot_historical_player_stats = pd.concat([sot_historical_player_stats, total_sot_df])
        sot_historical_match_stats = pd.concat([sot_historical_match_stats, match_df])

    
# sot_historical_player_stats.to_csv(f'/Users/tylermartins/Documents/PitchSights/ps-betting/strategies/anytime_goalscorer/data/raw/ags_player_stats_{league}_{season}.csv')
# sot_historical_match_stats.to_csv(f'/Users/tylermartins/Documents/PitchSights/ps-betting/strategies/anytime_goalscorer/data/raw/ags_match_stats_{league}_{season}.csv')

    # home_df
    # combined_match_details
