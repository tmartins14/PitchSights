import re
from bs4 import BeautifulSoup
from typing import Tuple, Optional

def parse_score(score_text):
    parts = re.split(r'[-–]', score_text)
    return (int(parts[0].strip()), int(parts[1].strip())) if len(parts) == 2 else (None, None)

def extract_date_from_soup(soup: BeautifulSoup):
    date_div = soup.find('div', class_='scorebox_meta')
    date_tag = date_div.find('a') if date_div else None
    return date_tag.text.strip() if date_tag else (date_div.text.strip() if date_div else None)

def rename_team_columns(df, side):
    prefix = 'home_' if side == 'home' else 'away_'
    opp_prefix = 'away_' if side == 'home' else 'home_'
    renamed = {}
    for col in df.columns:
        if col == f"{prefix}team":
            renamed[col] = "team"
        elif col == f"{opp_prefix}team":
            renamed[col] = "opp"
        elif col.startswith(prefix):
            renamed[col] = col.replace(prefix, "team_")
        elif col.startswith(opp_prefix):
            renamed[col] = col.replace(opp_prefix, "opp_")
    return df.rename(columns=renamed)

def parse_score(score_text: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Parse '1-0', '2 – 2', etc. Returns (home, away) or (None, None).
    """
    if not score_text:
        return (None, None)
    parts = re.split(r"\s*[-–]\s*", score_text.strip())
    if len(parts) != 2:
        return (None, None)
    try:
        return (int(parts[0]), int(parts[1]))
    except ValueError:
        return (None, None)