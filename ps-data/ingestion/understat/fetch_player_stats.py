# ingestion/understat/fetch_player_stats.py
from __future__ import annotations

import os
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
from ingestion.understat.client_async import (
    make_client,
    close_client,
    get_match_shots,
    season_label,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw" / "understat"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Treat these Understat "result" values as shots on target
SOT_RESULTS = {"Goal", "SavedShot"}


def _aggregate_players(shots_side: List[Dict[str, Any]], side_label: str) -> pd.DataFrame:
    """
    Aggregate shooter- and assister-level stats for one side (home/away).
    Returns a DataFrame with columns:
      player_id, player, team_side, shots, shots_on_target, goals, xG, assists, xA
    """
    # Shooter aggregates
    rows: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for s in shots_side:
        name = (s.get("player") or "").strip()
        pid = str(s.get("player_id") or "")
        key = (pid, name)
        d = rows.setdefault(
            key,
            {
                "player_id": pid,
                "player": name,
                "team_side": side_label,
                "shots": 0,
                "shots_on_target": 0,
                "goals": 0,
                "xG": 0.0,
                "assists": 0,
                "xA": 0.0,
            },
        )
        d["shots"] += 1
        if s.get("result") in SOT_RESULTS:
            d["shots_on_target"] += 1
        if s.get("result") == "Goal" or s.get("isGoal") is True:
            d["goals"] += 1
        try:
            d["xG"] += float(s.get("xG") or 0.0)
        except Exception:
            pass

    # Assists/xA: attribute xA by matching "player_assisted" to a player's name on this side
    for s in shots_side:
        assister = (s.get("player_assisted") or "").strip()
        if not assister:
            continue
        is_goal = s.get("result") == "Goal" or s.get("isGoal") is True
        shot_xg = 0.0
        try:
            shot_xg = float(s.get("xG") or 0.0)
        except Exception:
            pass

        # Add assist/xA to the assister's row (same side)
        for (_pid, name), d in rows.items():
            if name == assister and d["team_side"] == side_label:
                if is_goal:
                    d["assists"] += 1
                d["xA"] += shot_xg
                break

    return pd.DataFrame(rows.values())


async def _one_match(us, match_id: str, season: Optional[str], league_slug: Optional[str]) -> pd.DataFrame:
    """
    Pulls Understat shots for a match_id and aggregates to per-player stats.
    """
    data = await get_match_shots(us, match_id)
    if not data:
        return pd.DataFrame()

    h = data.get("h") or []
    a = data.get("a") or []
    h_df = _aggregate_players(h, "home")
    a_df = _aggregate_players(a, "away")
    out = pd.concat([h_df, a_df], ignore_index=True)

    out.insert(0, "match_id", match_id)
    if season:
        out["season"] = season
    if league_slug:
        out["league_slug"] = league_slug

    # Column order preference
    wanted = [
        "match_id",
        "team_side",
        "player_id",
        "player",
        "shots",
        "shots_on_target",
        "goals",
        "xG",
        "assists",
        "xA",
        "season",
        "league_slug",
    ]
    cols = [c for c in wanted if c in out.columns] + [c for c in out.columns if c not in wanted]
    return out[cols]


async def main():
    # ENV config
    league_slug = os.getenv("LEAGUE_SLUG", "Premier-League")
    season_start_year = int(os.getenv("SEASON_START_YEAR", "2024"))
    season = season_label(season_start_year)  # e.g., "2024-2025"
    throttle = float(os.getenv("THROTTLE", "1.0"))

    fixtures_file = RAW_DIR / f"fixtures_{season}_{league_slug}.csv"
    out_file = RAW_DIR / f"player_stats_{season}_{league_slug}.csv"

    if not fixtures_file.exists():
        logging.error(f"No fixtures at {fixtures_file}. Run fetch_fixtures first.")
        raise SystemExit(1)

    fixtures = pd.read_csv(fixtures_file)
    # Only played matches
    played = fixtures.dropna(subset=["home_score", "away_score"]).copy()

    # For nice progress formatting
    total = len(played)
    logging.info(f"{total} played matches found.")

    # Incremental skip: if we already have rows for a match_id, skip reprocessing it
    done_ids: set[str] = set()
    if out_file.exists():
        try:
            prev = pd.read_csv(out_file, usecols=["match_id"])
            done_ids = set(prev["match_id"].astype(str).unique())
            logging.info(f"Skipping {len(done_ids)} previously processed matches.")
        except Exception:
            pass

    # Build some helpful labels for logging
    played_reset = played.reset_index(drop=True)

    session, us = await make_client()
    frames: List[pd.DataFrame] = []

    try:
        for n, (_, r) in enumerate(played_reset.iterrows(), start=1):
            mid = str(r["match_id"])
            if mid in done_ids:
                print(f"[{n}/{total}] SKIP match_id={mid} (already processed)", flush=True)
                continue

            home = r.get("home_team", "")
            away = r.get("away_team", "")
            when = r.get("match_date", "")

            print(
                f"[{n}/{total}] Fetching {when} — {home} vs {away} | match_id={mid}",
                flush=True,
            )

            pdf = await _one_match(us, mid, season, league_slug)
            print(f"  → got {len(pdf)} player rows", flush=True)

            if not pdf.empty:
                frames.append(pdf)

            # polite pause
            await asyncio.sleep(throttle)
    finally:
        await close_client(session)

    # Append new rows to existing file, and de-duplicate
    if frames:
        new_df = pd.concat(frames, ignore_index=True)
        if out_file.exists():
            try:
                old_df = pd.read_csv(out_file)
                combined = pd.concat([old_df, new_df], ignore_index=True)
            except Exception:
                combined = new_df
        else:
            combined = new_df

        # Dedup on match_id+player_id+team_side (safe key for per-player per match)
        dedup_cols = ["match_id", "player_id", "team_side"]
        keep_cols = [c for c in dedup_cols if c in combined.columns]
        if keep_cols:
            combined = combined.drop_duplicates(subset=keep_cols, keep="last")

        combined.to_csv(out_file, index=False)
        logging.info(f"✅ Wrote player stats → {out_file} ({len(combined)} rows total)")
    else:
        # Nothing new; ensure existing file presence is reported
        if out_file.exists():
            try:
                rows = len(pd.read_csv(out_file))
            except Exception:
                rows = 0
            logging.info(f"✅ No new matches. Existing file unchanged → {out_file} ({rows} rows)")
        else:
            logging.info("No player stats to write (no frames accumulated and no prior file).")


if __name__ == "__main__":
    asyncio.run(main())
