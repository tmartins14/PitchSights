from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup

from ingestion.utils import fetch_url_with_backoff, write_raw

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


def _extract_match_date(soup: BeautifulSoup) -> Optional[str]:
    meta = soup.find("div", class_="scorebox_meta")
    if not meta:
        return None
    a = meta.find("a")
    if a and a.get_text(strip=True):
        return a.get_text(strip=True)
    return meta.get_text(strip=True) if meta.get_text(strip=True) else None


def _merge_player_tables(soup: BeautifulSoup) -> List[Dict]:
    """
    Merge 'summary' + 'possession' player tables for both teams into one list of dicts.
    """
    import re
    merged: Dict[tuple, Dict] = {}
    match_date = _extract_match_date(soup)

    # mapping of table-id fragments to wanted columns
    wanted = {
        "summary": [
            "player", "position", "age", "minutes",
            "goals", "shots", "shots_on_target", "xg", "npxg",
            "xg_assist", "sca", "gca", "pens_att",
        ],
        "possession": [
            "miscontrols", "dispossessed", "passes_received",
            "progressive_passes_received", "take_ons", "take_ons_won",
            "take_ons_tackled", "touches_att_3rd", "touches_att_pen_area",
        ],
    }

    player_divs = soup.find_all("div", id=re.compile(r"all_player_stats"))
    for div in player_divs:
        # team name appears in the h2 above the tables
        h2 = div.find("h2")
        team_name = h2.get_text(strip=True).replace(" Player Stats", "") if h2 else None

        for section, fields in wanted.items():
            tables = div.find_all("table", id=re.compile(section))
            for table in tables:
                tbody = table.find("tbody")
                if not tbody:
                    continue
                for row in tbody.find_all("tr"):
                    th = row.find("th", {"data-stat": "player"})
                    if not th:
                        continue
                    player_name = th.get_text(strip=True)
                    key = (player_name, team_name)
                    d = merged.setdefault(key, {"player": player_name, "team": team_name, "match_date": match_date})
                    for col in fields:
                        if col == "player":
                            continue
                        td = row.find("td", {"data-stat": col})
                        if td:
                            d[col] = td.get_text(strip=True)

    return list(merged.values())


def fetch_player_stats_from_soup(soup: BeautifulSoup) -> pd.DataFrame:
    rows = _merge_player_tables(soup)
    return pd.DataFrame(rows)


def fetch_player_stats(match_url: Optional[str] = None,
                       soup: Optional[BeautifulSoup] = None,
                       season: Optional[str] = None,
                       league_slug: Optional[str] = None) -> pd.DataFrame:
    if soup is None:
        if not match_url:
            return pd.DataFrame()
        resp = fetch_url_with_backoff(match_url, timeout=45)
        if not resp:
            return pd.DataFrame()
        soup = BeautifulSoup(resp.text, "html.parser")

    df = fetch_player_stats_from_soup(soup)
    if not df.empty:
        if season is not None: df["season"] = season
        if league_slug is not None: df["league_slug"] = league_slug
    return df


def run_from_fixtures(season: str, league_slug: str, throttle: float = 5.0) -> None:
    """
    Incremental player-stats scrape:
    - only for fixtures with scores
    - skip match_ids already present in player_stats.csv
    - write if changed
    """
    import time

    fixtures_file = RAW_DIR / f"fixtures_{season}_{league_slug}.csv"
    if not fixtures_file.exists():
        logging.warning("fixtures not found. Run fetch_fixtures first.")
        return

    player_file = RAW_DIR / f"player_stats_{season}_{league_slug}.csv"
    done = set()
    if player_file.exists():
        try:
            done = set(pd.read_csv(player_file, usecols=["match_id"])["match_id"].astype(str))
        except Exception:
            done = set()

    fixtures = pd.read_csv(fixtures_file)
    played = fixtures[fixtures["home_score"].notna() & fixtures["away_score"].notna()].copy()

    frames: List[pd.DataFrame] = []
    for i, r in enumerate(played.itertuples(index=False), start=1):
        mid = str(getattr(r, "match_id"))
        murl = getattr(r, "match_url")
        if not isinstance(murl, str) or not murl.strip():
            continue
        if mid in done:
            continue

        home = getattr(r, "home_team"); away = getattr(r, "away_team"); mdate = getattr(r, "match_date")
        logging.info(f"[{i}/{len(played)}] {mdate} — {home} vs {away} (match_id={mid})")

        pdf = fetch_player_stats(match_url=murl, season=season, league_slug=league_slug)
        if not pdf.empty:
            pdf["match_id"] = mid
            frames.append(pdf)
        time.sleep(throttle)

    if not frames:
        logging.info("No new player rows to write.")
        return

    new_df = pd.concat(frames, ignore_index=True)
    if player_file.exists():
        prev = pd.read_csv(player_file)
        new_df = pd.concat([prev, new_df], ignore_index=True)

    _save_if_changed(new_df, player_file)


def parse_args():
    p = argparse.ArgumentParser(description="Fetch FBRef player stats (incremental from fixtures)")
    p.add_argument("--season", default="2025-2026")
    p.add_argument("--league-slug", default="Premier-League")
    p.add_argument("--throttle", type=float, default=5.0)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_from_fixtures(args.season, args.league_slug, throttle=args.throttle)
