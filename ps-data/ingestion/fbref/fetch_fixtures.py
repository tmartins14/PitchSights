from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
from bs4 import BeautifulSoup

from ingestion.utils import fetch_url_with_backoff, parse_score, match_id_from_url, write_raw

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RAW_DIR = Path("data/raw/fbref")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def build_schedule_url(league_num: str, season: str, league_slug: str) -> str:
    return (
        f"https://fbref.com/en/comps/{league_num}/{season}/schedule/"
        f"{season}-{league_slug}-Scores-and-Fixtures"
    )


def _save_if_changed(df: pd.DataFrame, path: Path) -> bool:
    """
    Overwrite only if contents changed.
    Returns True if file written.
    """
    if path.exists():
        try:
            old = pd.read_csv(path)
            # Compare after aligning column order and NA types
            same_cols = sorted(df.columns) == sorted(old.columns)
            if same_cols:
                df_cmp = df.sort_values(list(sorted(df.columns))).reset_index(drop=True)
                old_cmp = old.sort_values(list(sorted(old.columns))).reset_index(drop=True)
                if df_cmp.equals(old_cmp):
                    logging.info(f"No changes detected → not overwriting {path}")
                    return False
        except Exception as e:
            logging.warning(f"Could not compare with existing file ({e}); will overwrite.")

    write_raw(df, path)  # make sure write_raw uses line_terminator='\n' internally
    logging.info(f"✅ Wrote {len(df)} rows → {path}")
    return True


def fetch_fixtures(schedule_url: str, season: Optional[str] = None, league_slug: Optional[str] = None) -> pd.DataFrame:
    """Scrape one schedule page (bs4-only)"""
    resp = fetch_url_with_backoff(schedule_url, timeout=45)
    if not resp:
        logging.error(f"Failed to fetch schedule: {schedule_url}")
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "html.parser")
    records: List[Dict] = []

    rows = soup.select("table tr") or soup.find_all("tr")
    for row in rows:
        date_td  = row.find("td", {"data-stat": "date"})
        home_td  = row.find("td", {"data-stat": "home_team"})
        away_td  = row.find("td", {"data-stat": "away_team"})
        if not (date_td and home_td and away_td):
            continue

        # Skip rows that are visually present but empty
        date_txt = (date_td.get_text(strip=True) if date_td else "") or ""
        home_txt = (home_td.get_text(strip=True) if home_td else "") or ""
        away_txt = (away_td.get_text(strip=True) if away_td else "") or ""
        if not date_txt or not home_txt or not away_txt:
            continue

        start_td  = row.find("td", {"data-stat": "start_time"})
        hxg_td    = row.find("td", {"data-stat": "home_xg"})
        axg_td    = row.find("td", {"data-stat": "away_xg"})
        score_td  = row.find("td", {"data-stat": "score"})
        report_td = row.find("td", {"data-stat": "match_report"})
        link = report_td.find("a", href=True) if report_td else None
        match_url = f"https://fbref.com{link['href']}" if link else None

        home_score = away_score = None
        if score_td and score_td.get_text(strip=True):
            home_score, away_score = parse_score(score_td.get_text(strip=True))

        rec = {
            "match_id":   match_id_from_url(match_url),
            "match_date": date_txt,
            "start_time": (start_td.get_text(strip=True) if start_td else None) or None,
            "home_team":  home_txt,
            "away_team":  away_txt,
            "home_xG":    float(hxg_td.get_text(strip=True)) if hxg_td and hxg_td.get_text(strip=True) else None,
            "away_xG":    float(axg_td.get_text(strip=True)) if axg_td and axg_td.get_text(strip=True) else None,
            "home_score": home_score,
            "away_score": away_score,
            "match_url":  match_url,
        }
        if season:      rec["season"] = season
        if league_slug: rec["league_slug"] = league_slug
        records.append(rec)

    df = pd.DataFrame.from_records(records)

    # --------- Sanitize to remove blank/empty lines ---------
    if not df.empty:
        # Strip whitespace from all string cells
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str).str.strip()

        # Convert empty strings to NA
        df.replace(r"^\s*$", pd.NA, regex=True, inplace=True)

        # Drop rows that are completely empty
        before_all_na = len(df)
        df = df.dropna(how="all")
        dropped_all_na = before_all_na - len(df)

        # Enforce required fields
        before_req = len(df)
        df = df.dropna(subset=["match_date", "home_team", "away_team"])
        dropped_req = before_req - len(df)

        # Optional: drop perfect duplicates
        before_dup = len(df)
        df = df.drop_duplicates()
        dropped_dup = before_dup - len(df)

        if dropped_all_na or dropped_req or dropped_dup:
            logging.info(
                f"Cleaned fixtures: -allNA={dropped_all_na}, -missing_required={dropped_req}, -dups={dropped_dup}"
            )

    return df


def parse_args():
    p = argparse.ArgumentParser(description="Fetch FBRef fixtures")
    p.add_argument("--league-num", default="9")
    p.add_argument("--season", default="2025-2026")
    p.add_argument("--league-slug", default="Premier-League")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    url = build_schedule_url(args.league_num, args.season, args.league_slug)
    df = fetch_fixtures(url, season=args.season, league_slug=args.league_slug)
    out = RAW_DIR / f"fixtures_{args.season}_{args.league_slug}.csv"
    _save_if_changed(df, out)
