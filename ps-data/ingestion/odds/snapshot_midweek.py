import argparse
from ingestion.odds.the_odds_api import run_odds_snapshot

def parse_args():
    p = argparse.ArgumentParser(description="Snapshot MIDWEEK odds (48–72h before KO)")
    p.add_argument("--sport-key", default="soccer_epl")
    p.add_argument("--league-slug", default="Premier-League")
    p.add_argument("--regions", default="uk,eu,us")
    p.add_argument("--bookmakers", default="")
    p.add_argument("--markets", default="h2h,spreads,totals,player_goal_scorer_anytime,player_shots_on_target")
    p.add_argument("--season", default=None)
    p.add_argument("--cache-ttl", type=int, default=3*60*60)  # 3h
    p.add_argument("--outfile", default="")

    return p.parse_args()

if __name__ == "__main__":
    a = parse_args()
    out = a.outfile or None
    run_odds_snapshot(
        snapshot_type="midweek",
        sport_key=a.sport_key,
        league_slug=a.league_slug,
        regions=a.regions,
        bookmakers=(a.bookmakers or None),
        markets=[m.strip() for m in a.markets.split(",") if m.strip()],
        cache_ttl=a.cache_ttl,
        window_min_hours=48,    # 2–3 days
        window_max_hours=72,
        season=a.season,
        dedup_mode="earliest",
    )
