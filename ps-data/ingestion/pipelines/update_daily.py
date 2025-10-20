"""
Unified daily pipeline for FBRef + Understat.

What it does
------------
1) FBRef stage (optional)
   - fetch fixtures → data/raw/fbref/fixtures_{season}_{league}.csv
   - for played matches, fetch each match page once → parse team+player → data/raw/fbref/
   - normalize → data/interim/fbref

2) Understat stage (optional)
   - fetch fixtures → data/raw/understat/fixtures_{season}_{league}.csv
   - fetch match shots → aggregate team+player → data/raw/understat/
   - normalize → data/interim/understat

3) Combined views (optional)
   - outer-join fixtures and team stats across sources
   - join key: match_date + normalized(home/away team)

Run examples
------------
# FBRef only
python -m ingestion.pipelines.update_daily \
  --with-fbref \
  --league-num 9 --season 2024-2025 --league-slug Premier-League \
  --fbref-throttle 8

# Understat only
python -m ingestion.pipelines.update_daily \
  --with-understat \
  --under-league-slug Premier-League --under-season-start 2024 --under-throttle 1.0

# Both + combined
python -m ingestion.pipelines.update_daily \
  --with-fbref --with-understat --with-combined \
  --league-num 9 --season 2024-2025 --league-slug Premier-League \
  --under-league-slug Premier-League --under-season-start 2024
"""

from __future__ import annotations

import os
import argparse
import asyncio
import logging
import random
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd
from bs4 import BeautifulSoup

# ---- FBRef imports ----
from ingestion.fbref.fetch_fixtures import fetch_fixtures as fbref_fetch_fixtures
from ingestion.fbref.fetch_team_stats import fetch_team_stats as fbref_fetch_team_stats
from ingestion.fbref.fetch_player_stats import fetch_player_stats as fbref_fetch_player_stats
from ingestion.fbref.normalize import (
    normalize_fixtures as fbref_normalize_fixtures,
    normalize_team_stats as fbref_normalize_team,
    normalize_player_stats as fbref_normalize_player,
)
from ingestion.utils import write_raw, write_interim, match_id_from_url, fetch_url_with_backoff

# ---- Understat imports (async client) ----
from ingestion.understat.client_async import (
    make_client, close_client, get_league_fixtures, get_match_shots,
    coerce_fixture_row, season_label,
)
from ingestion.understat.normalize import (
    normalize_fixtures as ustat_normalize_fixtures,
    normalize_team_stats as ustat_normalize_team,
    normalize_player_stats as ustat_normalize_player,
)

# ---- Team-name canonicalizer for cross-source joins ----
from ingestion.utils.team_names_mapping import normalize_team_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DATA_DIR = Path("data")
RAW_FBREF = DATA_DIR / "raw" / "fbref"
RAW_USTAT = DATA_DIR / "raw" / "understat"
INT_FBREF = DATA_DIR / "interim" / "fbref"
INT_USTAT = DATA_DIR / "interim" / "understat"
INT_COMBINED = DATA_DIR / "interim" / "combined"
for p in (RAW_FBREF, RAW_USTAT, INT_FBREF, INT_USTAT, INT_COMBINED):
    p.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def build_fbref_schedule_url(league_num: str, season: str, league_slug: str) -> str:
    return (
        f"https://fbref.com/en/comps/{league_num}/{season}/schedule/"
        f"{season}-{league_slug}-Scores-and-Fixtures"
    )

def _read_existing_ids(csv_path: Path, col: str) -> set:
    if not csv_path.exists():
        return set()
    try:
        return set(pd.read_csv(csv_path, usecols=[col])[col].astype(str))
    except Exception:
        return set()

def _jitter_sleep(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds + random.uniform(0, 1.25))

def _assign_season_league(df: pd.DataFrame, season: Optional[str], league_slug: Optional[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if season is not None:
        df["season"] = season
    if league_slug is not None:
        df["league_slug"] = league_slug
    return df

def _build_match_key(date_str: str, home: str, away: str) -> str:
    # canonicalized join key for cross-source merge
    h = normalize_team_name(home)
    a = normalize_team_name(away)
    d = pd.to_datetime(date_str, errors="coerce")
    return f"{d.date() if pd.notna(d) else date_str}__{h}__{a}"


# -------------------------------------------------------------------
# FBRef stage
# -------------------------------------------------------------------
def run_fbref_stage(
    league_num: str,
    season: str,
    league_slug: str,
    filter_date: Optional[str],
    throttle_seconds: float,
    cache_ttl: int = 6 * 3600,
) -> Dict[str, Path]:
    """
    Fetch fixtures, then for played matches fetch a SINGLE match page and parse team+player; write raw + normalize.
    Files:
      data/raw/fbref/fixtures_{season}_{league_slug}.csv
      data/raw/fbref/team_stats_{season}_{league_slug}.csv
      data/raw/fbref/player_stats_{season}_{league_slug}.csv
    """
    out_paths: Dict[str, Path] = {}
    schedule_url = build_fbref_schedule_url(league_num, season, league_slug)

    # 1) Fixtures
    fixtures = fbref_fetch_fixtures(schedule_url, season=season, league_slug=league_slug, cache_ttl=cache_ttl)
    if fixtures.empty:
        logging.warning("[FBRef] No fixtures returned.")
        return out_paths

    if filter_date:
        fixtures["match_date"] = pd.to_datetime(fixtures["match_date"], errors="coerce")
        fixtures = fixtures[fixtures["match_date"] <= pd.to_datetime(filter_date)]
        logging.info(f"[FBRef] Filtered fixtures to <= {filter_date}: {len(fixtures)}")

    raw_fx = RAW_FBREF / f"fixtures_{season}_{league_slug}.csv"
    write_raw(fixtures, raw_fx)
    out_paths["fixtures"] = raw_fx

    # Normalize fixtures
    try:
        fx_inter = fbref_normalize_fixtures(raw_fx)
        write_interim(fx_inter, INT_FBREF / "fixtures.csv")
    except Exception as e:
        logging.exception(f"[FBRef] normalize fixtures failed: {e}")

    # 2) Team + Player (played matches only), incremental
    played = fixtures[fixtures["home_score"].notna() & fixtures["away_score"].notna()].copy()
    if played.empty:
        logging.info("[FBRef] No played matches to scrape team/player.")
        return out_paths

    raw_team   = RAW_FBREF / f"team_stats_{season}_{league_slug}.csv"
    raw_player = RAW_FBREF / f"player_stats_{season}_{league_slug}.csv"
    done_team   = _read_existing_ids(raw_team, "match_id")
    done_player = _read_existing_ids(raw_player, "match_id")

    team_rows: List[Dict[str, Any]] = []
    player_frames: List[pd.DataFrame] = []

    for i, row in enumerate(played.itertuples(index=False), start=1):
        match_url = getattr(row, "match_url", None)
        if not isinstance(match_url, str) or not match_url.strip():
            continue

        mid   = match_id_from_url(match_url) or ""
        home  = getattr(row, "home_team")
        away  = getattr(row, "away_team")
        mdate = getattr(row, "match_date")
        logging.info(f"[FBRef] [{i}/{len(played)}] {mdate} — {home} vs {away} (match_id={mid})")

        # One network fetch for the page; pass soup to both parsers.
        resp = fetch_url_with_backoff(
            match_url,
            timeout=45,
            cache_ttl=cache_ttl,
            allow_stale_on_error=True,
        )
        if not resp:
            logging.warning(f"[FBRef] skip match_id={mid} (fetch failed)")
            _jitter_sleep(throttle_seconds)
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Team stats (incremental)
        if mid not in done_team:
            tstats = fbref_fetch_team_stats(soup=soup, season=season, league_slug=league_slug)
            if tstats:
                tstats["match_id"] = mid
                team_rows.append(tstats)

        # Player stats (incremental)
        if mid not in done_player:
            pdf = fbref_fetch_player_stats(soup=soup, season=season, league_slug=league_slug)
            if not pdf.empty:
                pdf["match_id"] = mid
                player_frames.append(pdf)

        _jitter_sleep(throttle_seconds)

    # Write/append + normalize
    if team_rows:
        new_team = pd.DataFrame(team_rows)
        new_team = _assign_season_league(new_team, season, league_slug)
        if raw_team.exists():
            prev = pd.read_csv(raw_team)
            new_team = pd.concat([prev, new_team], ignore_index=True)
            new_team = new_team.drop_duplicates(subset=["match_id","team_side"], keep="last")
        write_raw(new_team, raw_team)
        out_paths["team"] = raw_team
        try:
            inter = fbref_normalize_team(raw_team)
            write_interim(inter, INT_FBREF / "team_stats.csv")
        except Exception as e:
            logging.exception(f"[FBRef] normalize team failed: {e}")
    else:
        logging.info("[FBRef] No new team rows.")

    if player_frames:
        new_player = pd.concat(player_frames, ignore_index=True)
        new_player = _assign_season_league(new_player, season, league_slug)
        if raw_player.exists():
            prev = pd.read_csv(raw_player)
            new_player = pd.concat([prev, new_player], ignore_index=True)
            # a reasonable dedupe: match_id + team_side + player (or player_id if present)
            key_cols = ["match_id", "team_side"]
            if "player_id" in new_player.columns:
                key_cols.append("player_id")
            else:
                key_cols.append("player")
            new_player = new_player.drop_duplicates(subset=key_cols, keep="last")
        write_raw(new_player, raw_player)
        out_paths["player"] = raw_player
        try:
            inter = fbref_normalize_player(raw_player)
            write_interim(inter, INT_FBREF / "player_stats.csv")
        except Exception as e:
            logging.exception(f"[FBRef] normalize player failed: {e}")
    else:
        logging.info("[FBRef] No new player rows.")

    return out_paths


# -------------------------------------------------------------------
# Understat stage (async)
# -------------------------------------------------------------------
async def run_understat_stage_async(
    league_slug: str,
    season_start_year: int,
    filter_date: Optional[str],
    throttle: float,
) -> Dict[str, Path]:
    """
    Fetch fixtures, shots → team/player aggregates, write raw + normalize.
    Files:
      data/raw/understat/fixtures_{season}_{league_slug}.csv
      data/raw/understat/team_stats_{season}_{league_slug}.csv
      data/raw/understat/player_stats_{season}_{league_slug}.csv
    """
    out_paths: Dict[str, Path] = {}
    season = season_label(season_start_year)

    fixtures_path = RAW_USTAT / f"fixtures_{season}_{league_slug}.csv"
    team_path     = RAW_USTAT / f"team_stats_{season}_{league_slug}.csv"
    player_path   = RAW_USTAT / f"player_stats_{season}_{league_slug}.csv"

    session, us = await make_client()
    try:
        # Fixtures
        raw_fx = await get_league_fixtures(us, league_slug, season_start_year)
        rows = [coerce_fixture_row(m, season, league_slug) for m in (raw_fx or [])]
        fixtures = pd.DataFrame(rows)
        if fixtures.empty:
            logging.warning("[Understat] No fixtures.")
            return out_paths

        if filter_date:
            fixtures["match_date"] = pd.to_datetime(fixtures["match_date"], errors="coerce")
            fixtures = fixtures[fixtures["match_date"] <= pd.to_datetime(filter_date)]
            logging.info(f"[Understat] Filtered to <= {filter_date}: {len(fixtures)}")

        write_raw(fixtures, fixtures_path)
        out_paths["fixtures"] = fixtures_path

        # Played matches
        played = fixtures.dropna(subset=["home_score", "away_score"])
        if played.empty:
            logging.info("[Understat] No played matches.")
            return out_paths

        done_team   = _read_existing_ids(team_path, "match_id")
        done_player = _read_existing_ids(player_path, "match_id")

        SOT = {"Goal", "SavedShot"}

        def _team_agg(shots: List[Dict[str, Any]]) -> Dict[str, Any]:
            shots_count = len(shots)
            sot_count = sum(1 for s in shots if (s.get("result") in SOT))
            goals = sum(1 for s in shots if (s.get("result") == "Goal" or s.get("isGoal") is True))
            xg = 0.0
            for s in shots:
                try:
                    xg += float(s.get("xG") or 0.0)
                except Exception:
                    pass
            return dict(team_shots=shots_count, team_shots_on_target=sot_count, team_goals=goals, team_xG=xg)

        def _players_agg(shots: List[Dict[str, Any]], side_label: str) -> pd.DataFrame:
            rows = {}
            for s in shots:
                name = (s.get("player") or "").strip()
                pid  = str(s.get("player_id") or "")
                key = pid or name
                d = rows.setdefault(key, {"player_id": pid, "player": name, "team_side": side_label,
                                          "shots": 0, "shots_on_target": 0, "goals": 0, "xG": 0.0,
                                          "assists": 0, "xA": 0.0})
                d["shots"] += 1
                if s.get("result") in SOT:
                    d["shots_on_target"] += 1
                if s.get("result") == "Goal" or s.get("isGoal") is True:
                    d["goals"] += 1
                try:
                    d["xG"] += float(s.get("xG") or 0.0)
                except Exception:
                    pass

            # naive assist/xA accumulation
            for s in shots:
                assister = (s.get("player_assisted") or "").strip()
                if not assister:
                    continue
                is_goal = s.get("result") == "Goal" or s.get("isGoal") is True
                shot_xg = 0.0
                try:
                    shot_xg = float(s.get("xG") or 0.0)
                except Exception:
                    pass
                for d in rows.values():
                    if d["player"] == assister and d["team_side"] == side_label:
                        if is_goal:
                            d["assists"] += 1
                        d["xA"] += shot_xg
                        break

            return pd.DataFrame(rows.values())

        team_frames: List[pd.DataFrame] = []
        player_frames: List[pd.DataFrame] = []

        for i, r in enumerate(played.itertuples(index=False), start=1):
            mid = str(getattr(r, "match_id"))
            home = getattr(r, "home_team")
            away = getattr(r, "away_team")
            mdate = getattr(r, "match_date")
            logging.info(f"[Understat] [{i}/{len(played)}] {mdate} — {home} vs {away} (match_id={mid})")

            data = await get_match_shots(us, mid)
            if not data:
                await asyncio.sleep(throttle)
                continue

            h = data.get("h") or []
            a = data.get("a") or []

            if mid not in done_team:
                h_agg = _team_agg(h); a_agg = _team_agg(a)
                tdf = pd.DataFrame([
                    {"match_id": mid, "team_side": "home", "team": home, **h_agg},
                    {"match_id": mid, "team_side": "away", "team": away, **a_agg},
                ])
                tdf["season"] = season; tdf["league_slug"] = league_slug
                team_frames.append(tdf)

            if mid not in done_player:
                hdf = _players_agg(h, "home"); adf = _players_agg(a, "away")
                pdf = pd.concat([hdf, adf], ignore_index=True)
                pdf.insert(0, "match_id", mid)
                pdf["season"] = season; pdf["league_slug"] = league_slug
                player_frames.append(pdf)

            await asyncio.sleep(throttle)

        if team_frames:
            new_team = pd.concat(team_frames, ignore_index=True)
            if team_path.exists():
                new_team = pd.concat([pd.read_csv(team_path), new_team], ignore_index=True)
                new_team = new_team.drop_duplicates(subset=["match_id","team_side"], keep="last")
            write_raw(new_team, team_path)
            out_paths["team"] = team_path
            try:
                inter = ustat_normalize_team(season, league_slug)
                _ = inter
            except Exception as e:
                logging.exception(f"[Understat] normalize team failed: {e}")
        else:
            logging.info("[Understat] No new team rows.")

        if player_frames:
            new_player = pd.concat(player_frames, ignore_index=True)
            if player_path.exists():
                new_player = pd.concat([pd.read_csv(player_path), new_player], ignore_index=True)
                key_cols = ["match_id", "team_side"]
                if "player_id" in new_player.columns:
                    key_cols.append("player_id")
                else:
                    key_cols.append("player")
                new_player = new_player.drop_duplicates(subset=key_cols, keep="last")
            write_raw(new_player, player_path)
            out_paths["player"] = player_path
            try:
                inter = ustat_normalize_player(season, league_slug)
                _ = inter
            except Exception as e:
                logging.exception(f"[Understat] normalize player failed: {e}")
        else:
            logging.info("[Understat] No new player rows.")

        try:
            ustat_normalize_fixtures(season, league_slug)
        except Exception as e:
            logging.exception(f"[Understat] normalize fixtures failed: {e}")

        return out_paths

    finally:
        await close_client(session)


# -------------------------------------------------------------------
# Combined views
# -------------------------------------------------------------------
def _load_if_exists(path: Path, required_cols: Optional[List[str]] = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if required_cols:
        for c in required_cols:
            if c not in df.columns:
                return pd.DataFrame()
    return df

def build_combined_views(season_fb: str, league_fb: str,
                         season_us: str, league_us: str) -> None:
    """Outer-join FBRef and Understat into combined fixtures/team tables."""
    fb_fx_path = RAW_FBREF / f"fixtures_{season_fb}_{league_fb}.csv"
    us_fx_path = RAW_USTAT / f"fixtures_{season_us}_{league_us}.csv"

    fb_fx = _load_if_exists(fb_fx_path, ["match_date","home_team","away_team"])
    us_fx = _load_if_exists(us_fx_path, ["match_date","home_team","away_team"])

    if fb_fx.empty and us_fx.empty:
        logging.info("[Combined] No fixture files to combine.")
        return

    def add_key(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        df["__key__"] = df.apply(
            lambda r: _build_match_key(str(r["match_date"]), str(r["home_team"]), str(r["away_team"])), axis=1
        )
        return df

    fb_fx = add_key(fb_fx); us_fx = add_key(us_fx)

    fixtures_comb = pd.merge(
        fb_fx.add_prefix("fbref_"), us_fx.add_prefix("ustat_"),
        left_on="fbref___key__", right_on="ustat___key__", how="outer"
    )

    fixtures_comb["match_key"] = fixtures_comb["fbref___key__"].fillna(fixtures_comb["ustat___key__"])
    fixtures_comb.drop(columns=[c for c in fixtures_comb.columns if c.endswith("___key__")], inplace=True)

    INT_COMBINED.mkdir(parents=True, exist_ok=True)
    fixtures_out = INT_COMBINED / "fixtures_combined.csv"
    write_interim(fixtures_comb, fixtures_out)
    logging.info(f"✅ Combined fixtures → {fixtures_out} ({len(fixtures_comb)} rows)")

    # Team stats (join by match_key + team)
    fb_team_path = RAW_FBREF / f"team_stats_{season_fb}_{league_fb}.csv"
    us_team_path = RAW_USTAT / f"team_stats_{season_us}_{league_us}.csv"

    fb_team = _load_if_exists(fb_team_path, ["match_id","team","team_side"])
    us_team = _load_if_exists(us_team_path, ["match_id","team","team_side"])

    if fb_team.empty and us_team.empty:
        logging.info("[Combined] No team stat files to combine.")
        return

    # map match_id → match_key using fixtures
    fb_map = fixtures_comb[["match_key","fbref_match_id"]].rename(columns={"fbref_match_id":"match_id"}).dropna()
    us_map = fixtures_comb[["match_key","ustat_match_id"]].rename(columns={"ustat_match_id":"match_id"}).dropna()

    def attach_key(df: pd.DataFrame, mp: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        out = df.merge(mp, on="match_id", how="left")
        out["team_norm"] = out["team"].map(lambda t: normalize_team_name(str(t)))
        return out

    fb_team = attach_key(fb_team, fb_map).add_prefix("fbref_")
    us_team = attach_key(us_team, us_map).add_prefix("ustat_")

    team_comb = pd.merge(
        fb_team, us_team,
        left_on=["fbref_match_key","fbref_team_norm","fbref_team_side"],
        right_on=["ustat_match_key","ustat_team_norm","ustat_team_side"],
        how="outer"
    )
    team_comb.rename(columns={
        "fbref_match_key":"match_key",
        "fbref_team_norm":"team_norm",
        "fbref_team_side":"team_side",
    }, inplace=True)

    team_out = INT_COMBINED / "team_stats_combined.csv"
    write_interim(team_comb, team_out)
    logging.info(f"✅ Combined team stats → {team_out} ({len(team_comb)} rows)")
    # Player combined can be added later if needed.


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Unified FBRef + Understat daily pipeline")

    # Toggles
    p.add_argument("--with-fbref", action="store_true", help="Run FBRef stage")
    p.add_argument("--with-understat", action="store_true", help="Run Understat stage")
    p.add_argument("--with-combined", action="store_true", help="Build combined views (needs at least one source)")

    # FBRef params
    p.add_argument("--league-num", type=str, default=os.getenv("LEAGUE_NUM", "9"))
    p.add_argument("--season", type=str, default=os.getenv("SEASON", "2024-2025"))
    p.add_argument("--league-slug", type=str, default=os.getenv("LEAGUE_SLUG", "Premier-League"))
    p.add_argument("--fbref-filter-date", type=str, default=None)
    p.add_argument("--fbref-throttle", type=float, default=8.0)
    p.add_argument("--fbref-cache-ttl", type=int, default=6*3600)

    # Understat params
    p.add_argument("--under-league-slug", type=str, default=os.getenv("UNDER_LEAGUE_SLUG", "Premier-League"))
    p.add_argument("--under-season-start", type=int, default=int(os.getenv("UNDER_SEASON_START_YEAR", "2024")))
    p.add_argument("--under-filter-date", type=str, default=None)
    p.add_argument("--under-throttle", type=float, default=1.0)

    return p.parse_args()


def main():
    args = parse_args()

    season_fb, league_fb = args.season, args.league_slug
    season_us = season_label(args.under_season_start)
    league_us = args.under_league_slug

    # 1) FBRef
    if args.with_fbref:
        run_fbref_stage(
            league_num=args.league_num,
            season=args.season,
            league_slug=args.league_slug,
            filter_date=args.fbref_filter_date,
            throttle_seconds=args.fbref_throttle,
            cache_ttl=args.fbref_cache_ttl,
        )

    # 2) Understat
    if args.with_understat:
        asyncio.run(
            run_understat_stage_async(
                league_slug=args.under_league_slug,
                season_start_year=args.under_season_start,
                filter_date=args.under_filter_date,
                throttle=args.under_throttle,
            )
        )

    # 3) Combined
    if args.with_combined:
        try:
            build_combined_views(
                season_fb=season_fb, league_fb=league_fb,
                season_us=season_us, league_us=league_us
            )
        except Exception as e:
            logging.exception(f"[Combined] failed: {e}")

    logging.info("Pipeline finished.")


if __name__ == "__main__":
    main()
