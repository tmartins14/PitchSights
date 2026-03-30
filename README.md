# PitchSights

**A Data Visualization and Analytics Platform for Football Storytelling.**

PitchSights is an analytics project built to tell compelling stories through football data. I'm no PhD or Data Science Researcher - just a data engineer and a fan of the game The goal here is to take well-established methods and metrics from the football analytics community and make them accessible, readable, and visually compelling.

This repository is organized as a modular Python library with a clean data pipeline, a growing suite of analyses, and a consistent visual identity. It is built to compound: every tool built feeds the next.

---

## Philosophy

The football analytics community has produced a remarkable body of research. Most of it is buried in academic papers, Jupyter notebooks, and static matplotlib charts. The methodology is sound. I'd like to contribute to the storytelling.

PitchSights exists at the intersection of engineering and narrative — taking what the research community has established and building the visualization layer that makes it land.

A few principles that guide my work:

- **Modularity over one-offs.** Every piece of code is built to be reused. A pitch renderer, a data loader, a metric — built once, referenced everywhere.
- **Understand before you build.** Each analysis is preceded by reading the source material, understanding the data, and forming a point of view on what story is worth telling.
- **See the pitch differently.** The goal is not to reproduce what already exists. It is to find the angle that helps someone understand the game in a way they couldn't before.
- **Honest about data constraints.** Free, publicly available data has real limitations. Those limitations are acknowledged clearly rather than papered over. In a perfect world, I could do it all.

---

## Repository Structure

```
pitchsights/
├── src/                        # Core modular Python library
│   ├── viz/                    # Pitch renderer, theme, annotations, export
│   ├── data/                   # Loaders, normalization, caching
│   └── metrics/                # xG, xT, PPDA, similarity, and more
│
├── analyses/                   # Completed analyses, organized by concept
│   ├── xg/
│   ├── shot_maps/
│   ├── passing_networks/
│   ├── pressing/
│   ├── set_pieces/
│   ├── player_similarity/
│   ├── recruitment/
│   ├── player_valuation/
│   ├── team_style/
│   └── goalkeeping/
│
├── data/                       # Raw and processed flat files (transitional)
├── notebooks/                  # Exploratory scratch work only
└── Makefile                    # Pipeline orchestration
```

The `src/` library is the foundation. Analyses in `analyses/` import from it. The `data/` layer is intended to be productionalized over time — moving from flat files to a proper database-backed pipeline.

The organization of JavaScript source code is to be determined as the viz layer matures. See the Stack section below.

---

## Data Sources

All analyses in this repository are built on publicly available, free data sources. The trade-offs of each are acknowledged openly.

| Source | What It Provides | Coverage | Limitations |
|---|---|---|---|
| [FBref](https://fbref.com) | Aggregated player & team stats, xG, progressive actions | Big 5 leagues + many more, 2017/18+ | Aggregated only — no shot-level or event-level data |
| [Understat](https://understat.com) | Shot-level xG with location data | Big 5 + EPL, 2014/15+ | Limited to top leagues, no event context beyond shots |
| [Transfermarkt](https://www.transfermarkt.com) | Transfer values, contract data, player metadata | Broad — hundreds of leagues | Valuations are estimates, not market prices |
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | Full event data with freeze frames | Narrow — select competitions and seasons only | Not viable as a primary source for broad analyses |
| [Stathead](https://stathead.com) | Historical and aggregated stats | Good historical depth | Requires subscription for full access |

> **A note on tactical and tracking data:** Analyses requiring coordinate-level tracking data (pitch control, pressing maps, ghosting) are not executable with free data. Where these concepts appear in the syllabus, the methodology is studied and documented. Implementation is deferred until academic or paid data access is available.

---

## Stack

PitchSights uses a two-layer architecture with a deliberate separation between data and visualization.

**Python** handles everything on the data side — ingestion, cleaning, normalization, and metric computation. The output of the Python pipeline is clean, analysis-ready JSON that the visualization layer consumes.

**D3 / Observable** handles the rendering layer entirely. Observable is used for prototyping and exploration; standalone D3 is used for production outputs. Neither layer needs to know the internals of the other.

| Layer | Language / Tool | Role |
|---|---|---|
| Data ingestion & pipeline | Python, Makefile | Scraping, cleaning, transformation |
| Metrics & modeling | Python (pandas, scikit-learn) | xG aggregation, similarity, normalization |
| Viz prototyping | Observable | Rapid iteration on chart designs |
| Viz production | D3.js | Standalone SVG/HTML outputs, eventual web app |

The organization of JavaScript source within the repository is to be determined as the visualization layer matures.

---

The following is the full curriculum for PitchSights, drawn primarily from the football analytics community — in particular the resource guide compiled by [Edd Webster](https://github.com/eddwebster/football_analytics), which serves as the foundational map for this work.

Each concept is studied before it is built. Reading, data familiarization, and a review of existing community work precede every analysis.

### Foundation
- Expected Goals (xG) — methodology, history, and limitations
- Radars & player profiles — percentile-based multi-metric summaries

### Shooting & Chance Creation
- Shot maps & shot quality profiles
- xG match narratives — how games unfold through expected goals

### Passing & Progression
- Passing networks — structure, hierarchy, and flow
- Progressive passing & ball carrying
- Possession value frameworks — xT, VAEP, OBV, g+

### Out of Possession
- Pressing intensity & PPDA
- Counter-pressing
- Defensive actions & chance denial

### Set Pieces
- Attacking set piece analysis
- Defensive set piece analysis

### Player Analysis
- Player similarity & comparables
- Player valuation & transfer value modeling
- Aging curves
- Recruitment analysis & league adjustment

### Team Analysis
- Team playing style & clustering
- Quantifying relative league strength
- Game win probability modeling

### Goalkeeping
- Shot stopping beyond save percentage
- Sweeping & distribution

### Aspirational (Requires Tracking Data)
- Pitch control modeling
- Off-ball movement & ghosting
- Pressing maps

---

## Completed Work

*This section is updated as analyses are built and published.*

Nothing here yet. The first analyses are in progress.

---

## Acknowledgements

This project stands on the shoulders of the football analytics community. In particular:

- **[Edd Webster](https://github.com/eddwebster/football_analytics)** — whose resource guide serves as the syllabus for this work
- **[StatsBomb](https://statsbomb.com)** — for open data and a decade of public-facing analytical writing
- **[Friends of Tracking](https://www.youtube.com/channel/UCUBFJYcag8j2rm_9HkrrA7w)** — for accessible education on tracking and event data methods
- The broader community of analysts, researchers, and engineers who have made their work public

---

## About

PitchSights is a personal project by a data and software engineer with a background in football analytics. It is built in public as a portfolio of analytical and engineering work.

Built with Python and D3 / Observable. Data sourced from publicly available providers. Visual identity and tooling are original.