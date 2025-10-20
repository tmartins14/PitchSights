from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd
from bs4 import BeautifulSoup

from ingestion.utils import fetch_url_with_backoff, match_id_from_url, write_raw

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RAW_DIR = Path("data/raw/fbref")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def _save_if_changed(df: pd.DataFrame, path: Path) -> bool:
    if path.exists():
        try:
            old = pd.read_csv(path)
            if sorted(df.columns) == sorted(old.columns):
                df_cmp = df.sort_values(list(sorted(df.columns))).reset_index(drop=True)
                old_cmp = old.sort_values(list(sorted(old.columns))).reset_index(drop=True)
                if df_cmp.equals(old_cmp):
                    logging.info(f"No changes detected → not overwriting {path}")
                    return False
        except Exception as e:
            logging.warning(f"Compare failed ({e}); will overwrite.")
    write_raw(df, path)
    logging.info(f"✅ Wrote {len(df)} rows → {path}")
    return True


def fetch_team_stats_from_soup(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """
    Scrape the match page 'Team Stats' widget for:
    shots_on_target (and total shots), saves (and shots faced),
    possession %, and passing accuracy % for home/away.
    """
    details = {
        "home_shots_on_target": None, "away_shots_on_target": None,
        "home_shots": None, "away_shots": None,
        "home_saves": None, "away_saves": None,
        "home_saves_faced": None, "away_saves_faced": None,
        "home_possession": None, "away_possession": None,
        "home_passing_accuracy": None, "away_passing_accuracy": None,
    }

    stats_div = soup.find("div", {"id": "team_stats"})
    if not stats_div:
        return None

    import re
    current_stat = None
    for row in stats_div.find_all("tr"):
        th = row.find("th", {"colspan": "2"})
        if th:
            current_stat = th.get_text(strip=True)
            continue

        tds = row.find_all("td")
        if len(tds) != 2:
            continue
        home_text, away_text = [td.get_text(strip=True) for td in tds]

        if current_stat == "Shots on Target":
            h = re.search(r"(\d+)\s*of\s*(\d+)", home_text)
            a = re.search(r"(\d+)\s*of\s*(\d+)", away_text)
            if h:
                details["home_shots_on_target"] = int(h.group(1))
                details["home_shots"] = int(h.group(2))
            if a:
                details["away_shots_on_target"] = int(a.group(1))
                details["away_shots"] = int(a.group(2))

        elif current_stat == "Saves":
            h = re.search(r"(\d+)\s*of\s*(\d+)", home_text)
            a = re.search(r"(\d+)\s*of\s*(\d+)", away_text)
            if h:
                details["home_saves"] = int(h.group(1))
                details["home_saves_faced"] = int(h.group(2))
            if a:
                details["away_saves"] = int(a.group(1))
                details["away_saves_faced"] = int(a.group(2))

        elif current_stat == "Possession":
            details["home_possession"] = home_text.replace("%", "").strip() or None
            details["away_possession"] = away_text.replace("%", "").strip() or None

        elif current_stat == "Passing Accuracy":
            hp = re.search(r"(\d{1,3})%", home_text)
            ap = re.search(r"(\d{1,3})%", away_text)
            if hp: details["home_passing_accuracy"] = int(hp.group(1))
            if ap: details["away_passing_accuracy"] = int(ap.group(1))

    return details


def fetch_team_stats(match_url: Optional[str] = None,
                     soup: Optional[BeautifulSoup] = None,
                     season: Optional[str] = None,
                     league_slug: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if soup is None:
        if not match_url:
            return None
        resp = fetch_url_with_backoff(match_url, timeout=45)
        if not resp:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")

    out = fetch_team_stats_from_soup(soup)
    if out is not None:
        if season is not None:
            out["season"] = season
        if league_slug is not None:
            out["league_slug"] = league_slug
    return out


def run_from_fixtures(season: str, league_slug: str, throttle: float = 5.0) -> None:
    """
    Incremental team-stats scrape based on fixtures.csv:
    - only for matches with a score
    - skip match_ids already present in team_stats.csv
    - write if changed
    """
    import time

    fixtures_file = RAW_DIR / f"fixtures_{season}_{league_slug}.csv"
    if not fixtures_file.exists():
        logging.warning("fixtures not found. Run fetch_fixtures first.")
        return

    team_file = RAW_DIR / f"team_stats_{season}_{league_slug}.csv"
    done = set()
    if team_file.exists():
        try:
            done = set(pd.read_csv(team_file, usecols=["match_id"])["match_id"].astype(str))
        except Exception:
            done = set()

    fixtures = pd.read_csv(fixtures_file)
    played = fixtures[fixtures["home_score"].notna() & fixtures["away_score"].notna()].copy()
    rows: List[Dict[str, Any]] = []

    for i, r in enumerate(played.itertuples(index=False), start=1):
        mid = str(getattr(r, "match_id"))
        murl = getattr(r, "match_url")
        if not isinstance(murl, str) or not murl.strip():
            continue
        if mid in done:
            continue

        home = getattr(r, "home_team"); away = getattr(r, "away_team"); mdate = getattr(r, "match_date")
        logging.info(f"[{i}/{len(played)}] {mdate} — {home} vs {away} (match_id={mid})")

        d = fetch_team_stats(match_url=murl, season=season, league_slug=league_slug)
        if d:
            d["match_id"] = mid
            rows.append(d)
        time.sleep(throttle)

    if not rows:
        logging.info("No new team rows to write.")
        return

    new_df = pd.DataFrame(rows)
    if team_file.exists():
        prev = pd.read_csv(team_file)
        new_df = pd.concat([prev, new_df], ignore_index=True)

    _save_if_changed(new_df, team_file)


def parse_args():
    p = argparse.ArgumentParser(description="Fetch FBRef team stats (incremental from fixtures)")
    p.add_argument("--season", default="2025-2026")
    p.add_argument("--league-slug", default="Premier-League")
    p.add_argument("--throttle", type=float, default=5.0)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_from_fixtures(args.season, args.league_slug, throttle=args.throttle)
