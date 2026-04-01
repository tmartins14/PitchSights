# Expected Goals (xG) in Football Analytics: A Literature Review

**PitchSights Analytics Syllabus — Review No. 1**
*Prepared: March 2026*

---

## Table of Contents

1. [Abstract](#1-abstract)
2. [Introduction](#2-introduction)
3. [Historical Development](#3-historical-development)
4. [Theoretical Foundations](#4-theoretical-foundations)
5. [Key Studies & Empirical Findings](#5-key-studies--empirical-findings)
6. [Methodological Variations & Debates](#6-methodological-variations--debates)
7. [Practical Applications](#7-practical-applications)
8. [Critiques & Limitations](#8-critiques--limitations)
9. [Future Directions](#9-future-directions)
10. [Conclusion](#10-conclusion)
11. [References](#11-references)

---

## 1. Abstract

Expected Goals (xG) is the foundational metric of modern football analytics: a probabilistic model that assigns each shot a value between 0 and 1 representing the historical likelihood that a shot from equivalent circumstances would result in a goal. By aggregating these values across a match or season, xG provides a result-agnostic measure of performance that is demonstrably more predictive of future outcomes than raw goal counts or shot totals. The concept has roots in the 1960s performance analysis of Charles Reep and Bernard Benjamin, but its modern form emerged from the practitioner community around 2012 and has since been adopted by elite clubs, broadcasters, betting operators, and independent analysts worldwide. This review traces xG's historical development from early shot-counting studies through to contemporary machine learning implementations, examines its mathematical foundations, evaluates the empirical evidence for its predictive validity, surveys the methodological debates over model architecture and feature selection, and critically assesses both its well-documented limitations and the emerging extensions — post-shot xG, positional adjustment, and tracking-data enrichment — that aim to address them.

---

## 2. Introduction

### Definition

Expected Goals (xG) is a statistical metric that models the probability that a given shot will result in a goal. Formally, it is a function that maps a feature vector describing a shot (location, body part, assist type, game state, defensive configuration, and others) to a scalar probability *p* ∈ (0, 1). A shot assigned an xG of 0.25 is interpreted as one that, across many historically similar attempts, would produce a goal approximately 25% of the time (Wikipedia, 2026). The sum of xG values across a set of shots — a match, half, or season — yields an *expected* goal tally that can be compared to the *actual* goals scored to assess luck, finishing quality, or goalkeeper performance.

xG is not a prediction about any single shot. It is a statement about a reference class of shots. This distinction is frequently misunderstood in public discourse and is central to interpreting the metric correctly.

### Why It Matters

Football is the world's lowest-scoring major team sport. With roughly one goal scored per ten shots and approximately 2.5 goals per match across top leagues, a small number of discrete outcomes governs the final scoreline. This creates enormous short-run variance: a team can dominate a match in process terms and lose, or concede from its opponent's only shot and win. Raw goal counts, therefore, carry substantial noise as performance indicators over small samples.

xG addresses this directly. By quantifying the *quality* of chances rather than their binary outcomes, xG gives analysts a signal that is both more stable and more informative than goals. At the team level, xG differential (xGD) has been shown to outperform goal difference in predicting future league position (Hudl StatsBomb, 2025). At the player level, xG per 90 minutes is a strongly persistent individual skill, with players' shot-location profiles tracking across club changes (Analytics FC, 2025). At the market level, xG values are used to calibrate betting odds and player valuations.

### Scope of This Review

This review covers: (a) pre-shot xG models, their historical development, mathematical underpinnings, and key empirical results; (b) post-shot xG (PSxG) as an extension for goalkeeper and finishing evaluation; (c) methodological debates over feature sets and model architecture; and (d) the metric's critiques and future directions. It does not cover possession-value frameworks (xT, OBV, EPV) or non-shot expected threat metrics, which are adjacent but distinct concepts treated separately in the PitchSights syllabus.

---

## 3. Historical Development

### Early Precursors (1960s–1990s)

The intellectual prehistory of xG begins with Charles Reep, an RAF Wing Commander and self-taught analyst who spent the period from 1952 to 1967 manually recording 667 matches, including four World Cups (These Football Times, 2020). Working with statistician Bernard Benjamin, Reep established with "startling uniformity" that it took approximately ten shots to score a goal — a rudimentary but structurally similar insight to what xG formalises. Their conclusions, however, were badly contaminated by the long-ball ideology Reep used to interpret them, and their work was later found to conflate correlation with causation on passing sequences (Pollard, 2019).

More directly relevant is the 1997 paper by Pollard and Reep, "Measuring the Effectiveness of Playing Strategies at Soccer," which considered distance, angle, foot vs. head, first-touch vs. not, and defensive proximity as factors in shot conversion — a feature set that anticipates modern xG models by fifteen years (Pollard & Reep, 1997, as cited in These Football Times, 2020). The term "expected goals" itself first appeared in print in a 1993 paper by Vic Barnett and Sarah Hilditch, who used it in a narrower sense to examine how artificial pitches affected home-team scoring rates — not in the sense of a per-shot probability model (Wikipedia, 2026).

Also in 2004, Ensum, Pollard, and Taylor applied logistic regression to shot data from the 1986 and 2002 World Cups, identifying five statistically significant predictors of shot success: distance, angle, distance from nearest defender, whether the shot was immediately preceded by a cross, and the number of outfield players between shooter and goal (Wikipedia, 2026). This was, in retrospect, a peer-reviewed proto-xG model — largely unnoticed by the emerging online analytics community.

### The Modern Era (2009–2015)

The modern xG framework emerged through the practitioner community, not the academic literature. Howard Hamilton proposed a per-action expected goal value in a 2009 blog post (Wikipedia, 2026). Sam Green of Opta is widely credited with developing the first recognisable modern xG model in 2012, proposing a model to assess "a shot's probability of being on target and/or scored" and coining the phrase "expected goal (xG) value" in its current sense (Wikipedia, 2026; These Football Times, 2020).

The period from 2012 to 2015 saw rapid proliferation through blogs and Twitter. Analysts including Michael Caley, Martin Eastwood, and Colin Trainor developed independent implementations, published them openly, and debated methodological choices in public. StatsBomb, founded by Ted Knutson, became a key nexus for this community and later a data provider. By 2015, xG had penetrated professional clubs and was beginning to appear in mainstream media coverage.

### Mainstream Adoption (2016–Present)

By the mid-2010s, virtually every top-flight European club had an analytics department engaging with xG. TV broadcasters including Sky Sports and the BBC began displaying xG graphics during match coverage. The metric appeared in football journalism at The Athletic and in the tactical analysis community on Substack. Platforms such as Understat, FBref (powered by StatsBomb data), and xGScore made the metric freely accessible to researchers and fans (StatsBomb, 2025). The journey from "nerd nonsense" — a phrase associated with early public scepticism — to mainstream ubiquity took roughly a decade.

---

## 4. Theoretical Foundations

### The Core Model

An xG model is, at its most fundamental level, a binary classification problem trained on a labelled dataset of historical shots (outcome: goal = 1, no goal = 0). The model learns to estimate P(goal | shot features) and outputs a probability for each shot.

The standard formulation using logistic regression is:

```
xG = 1 / (1 + e^-(β₀ + β₁x₁ + β₂x₂ + ... + βₙxₙ))
```

where x₁...xₙ are shot features (distance, angle, body part, etc.) and β₀...βₙ are coefficients estimated from training data. The logistic function constrains output to (0, 1), making it interpretable as a probability.

In practice, most contemporary implementations use ensemble methods — gradient boosted trees (XGBoost) or random forests — rather than logistic regression, because they handle non-linear feature interactions and do not require explicit specification of interaction terms (Anzer & Bauer, 2021; Robberechts & Davis, 2020 as cited in Bandara et al., 2024).

### Standard Features

The core features common across published models are:

- **Distance from goal** — typically the straight-line distance from the shot location to the centre of the goal; strongly negatively associated with conversion probability
- **Shot angle** — the angle subtended by the goal mouth from the shot location; positively associated with conversion probability
- **Body part** — headed shots convert at substantially lower rates than foot shots from equivalent positions
- **Assist type / phase of play** — shots from crosses, cutbacks, through-balls, set pieces, and open play have meaningfully different conversion rates even from the same location
- **Shot type** — open play vs. direct free kick vs. penalty; penalties are typically assigned a fixed value of approximately 0.76–0.78 based on the historical conversion rate

Advanced models additionally include goalkeeper position and status, the number and location of defenders between the shooter and goal, pressure on the shooter, and game state (scoreline, minute). StatsBomb's model — which uses freeze-frame data to record the location of all 22 players at the moment of the shot — allows for incorporation of several of these contextual variables that event-only models cannot access (Hudl StatsBomb, 2025).

### Key Assumptions

Three assumptions underpin xG modelling and are worth making explicit:

**Independence:** Shot probabilities are assumed to be estimated from a reference class of similar shots, with outcomes treated as independent draws. This is a reasonable approximation but ignores within-match dependencies (fatigue, goalkeeper confidence, momentum).

**Average player:** Standard xG models are trained on population-level shot data and therefore estimate the probability that an *average professional* would score from a given situation. They do not, by design, encode individual finishing skill — though positional- and player-adjusted extensions do (Robberechts & Davis, 2023; Davis & Robberechts, 2024).

**Stationarity:** Models assume the relationship between shot features and conversion probability is stable across leagues, eras, and contexts. Cross-league validity is imperfect; models trained on one competition may not generalise perfectly to another (Pratas et al., 2023).

---

## 5. Key Studies & Empirical Findings

### Predictive Validity of xG at the Team Level

The central empirical claim for xG is that it predicts future performance better than past results. Rathke (2017) examined approximately 18,000 shots from one season each of the Bundesliga and Premier League and found that xG differential was a better predictor of match outcomes than traditional metrics. Subsequent work broadly confirmed this finding across multiple leagues.

Pratas et al. (2023), using Wyscout data across the top five European leagues and comparing model families (logistic regression, random forest, gradient boosting, neural networks), found that ensemble methods consistently outperformed classical regression on held-out test data, and that xG-based models significantly outperformed traditional statistics (shot counts, goal difference) in predicting future outcomes (PLOS ONE, 2023).

At the team level, the strongest practical validation is the well-documented case of Brighton & Hove Albion under Graham Potter, who consistently generated high xG despite modest goal tallies in early seasons. The team's subsequent top-six Premier League finishes tracked the model's predictions rather than short-term results (MartinOnData, 2025).

### xG as a Shot-Positioning Metric for Players

Analytics FC (2025) found that a forward's xG per 90 is strongly persistent across seasons — with a persistence factor of approximately 0.6 for periods of 30+ matches — and that this persistence holds even when players switch clubs, suggesting the skill is individual rather than system-dependent. This result has important implications for recruitment: a player's xG per 90 is a reliable indicator of their ability to occupy dangerous positions, and this quality travels.

### Finishing Skill and the xG Overperformance Question

The most contested individual-level question in xG research is whether players can sustainably outperform their xG through superior finishing. The early practitioner consensus — sometimes called the "finishing is not a skill" position — held that Goals − xG was mostly noise.

More nuanced subsequent work challenged this. Sæbø & Hvattum (StatsBomb blog, 2019; see also Davis & Robberechts, 2024) used Bayesian hierarchical models and generalised linear mixed models with player random effects to show that finishing skill exists and is measurable, though it is hard to detect in small samples and requires careful adjustment for shot selection bias.

Davis & Robberechts (2024) formalised a key methodological problem: standard xG models are trained on population-level data, which means that if elite finishers disproportionately take shots, their shot locations are over-represented in the training set, creating a downward bias in the assigned xG for those locations and, consequently, an underestimate of such players' true Goals Above Expected. Using Messi as a case study, they found that a standard model underestimates his GAX by approximately 17%, and that his true finishing superiority over typical elite attackers is roughly 27% greater than standard xG implies (Davis & Robberechts, 2024).

Analytics FC (2025) found that finishing (Goals/xG) is *weakly* persistent, with a persistence factor of approximately 0.1 over 60-shot periods — meaning that if a player outperforms xG by one unit over 60 shots, we should expect roughly 0.1 units of outperformance to continue. The signal exists but is faint and easily swamped by variance.

### Post-Shot xG (PSxG)

StatsBomb introduced the post-shot xG framework in 2018, training a separate model on shots that were saved or scored (on target) to estimate the probability of scoring given the shot's observed trajectory, placement, and velocity — information available only after the ball has left the shooter's foot (StatsBomb blog archive, 2018). PSxG is by design uncorrected for goalkeeper positioning because repositioning is itself a goalkeeping skill to be measured, not controlled for.

The primary applications of PSxG are: (a) measuring goalkeeper shot-stopping ability via Goals Saved Above Average (GSAA = xG faced − goals conceded, or more precisely PSxG faced − goals conceded); and (b) measuring shooter execution quality beyond chance location. A PSxG consistently higher than pre-shot xG indicates a player who elevates average chances through superior ball-striking; the converse identifies a player who wastes good positions (The Sporting Blog, 2025).

---

## 6. Methodological Variations & Debates

### Event-Data vs. Tracking-Data Models

The fundamental methodological split in xG modelling is between models built on event data alone and those that incorporate positional (tracking) data.

**Event-data models** — including the Understat model, the historic Opta model, and many public implementations using StatsBomb open data — know where the shot was taken, with which body part, and from what type of assist, but do not know where defenders or the goalkeeper were positioned at the moment of the shot.

**Tracking-data models** — most prominently StatsBomb's freeze-frame model, Opta's 360 data implementation, and the Bundesliga model by Anzer & Bauer (2021) — incorporate the full spatial configuration of players at the moment of the shot. Anzer & Bauer (2021), using 105,627 shots from the German Bundesliga with synchronised positional data, achieved a Ranked Probability Score (RPS) of 0.197, lower (better) than any previously published model, attributing the improvement to contextual features including defender locations and goalkeeper positioning (Frontiers in Sports and Active Living, 2021).

The tradeoff is data availability and cost. Event-only data is available across hundreds of competitions; synchronised tracking data is expensive and currently available for only a handful of elite leagues.

### Model Architecture

Early models used logistic regression for its interpretability. Contemporary models use gradient boosted trees (XGBoost, LightGBM) or neural networks, which handle non-linear interactions without manual feature engineering. Multiple studies find that ensemble methods outperform logistic regression, particularly when spatial and contextual features are properly engineered (Pratas et al., 2023; Robberechts & Davis, 2020). However, logistic regression remains valuable for communication purposes: the coefficients have direct interpretability, and Sumpter et al. (2024) demonstrated how logistic regression coefficients can be translated directly into natural language shot descriptions via large language models (Sumpter et al., 2024, as cited in ResearchGate, 2024).

### Sequence-Based vs. Single-Shot Models

A growing line of research argues that single-shot xG models discard valuable information about the quality of the build-up. Bandara et al. (2024) proposed a framework incorporating temporal event sequences preceding a shot — the "advancement factor" of the attacking move and the spatial origin of the build-up — and found that a random forest trained on these features outperformed published single-event models (PMC, 2024). The intuition is that a shot following a sustained high-tempo sequence is qualitatively different from a speculative effort following a long clearance, even if the shot coordinates are identical.

The limitation, as Bandara et al. note, is that sequence features introduce complexity around defining the relevant preceding window, handling set-piece interruptions, and managing the resulting sparse feature space.

### Provider Disagreement

Because xG values are model outputs rather than physical measurements, different providers assign materially different probabilities to the same shot — particularly when they use different event definitions, different feature sets, or different training populations. Taber & Edwards (2024) compared Opta and Understat models across the top five European leagues from 2017–18 to 2023–24 and found that Understat outperformed Opta in terms of lower prediction errors in the Bundesliga, Premier League, and Serie A, while Opta yielded more stable predictions in La Liga and Ligue 1 (ResearchGate, 2024). This finding underscores that xG values from different providers are not directly comparable and should be treated as model outputs, not objective measurements.

### The Non-Shot Problem

A conceptually important critique of shot-based xG is that it assigns zero value to situations that are dangerous but do not result in a shot — a striker beating a defender to a through ball and being denied by a last-ditch tackle, for example. Fernández et al. (2021) and subsequent possession-value frameworks (xT, EPV, OBV) were developed in part to address this gap, though they constitute separate models rather than extensions of xG itself. A recent working paper (arxiv, 2025) proposed a joint framework modelling both the probability of creating a shot (xS) and converting it (xG), attributing credit to build-up play that standard shot-only xG ignores.

---

## 7. Practical Applications

### Recruitment

xG is the most widely used advanced metric in player recruitment. At the player level, xG per 90 serves as a chance-creation and shot-quality proxy for attackers; xG conceded per 90 serves as a defensive structure proxy for defenders. The metric allows cross-league comparison on a like-for-like basis, enabling clubs to identify forwards in lower-profile leagues who are generating high-quality chances even if raw goal totals do not reflect this. Brighton & Hove Albion under Tony Bloom and Graham Potter became the paradigmatic example of a club systematically exploiting xG-based recruitment to find value unavailable to clubs relying on traditional scouting (Tippett, 2019).

### Tactical Analysis

At the team level, xG differential (xGD) is used to assess process quality, identify overperforming or underperforming stretches, and diagnose structural issues. A team with a positive xGD but negative goal difference is likely to regress toward positive results; the converse indicates likely regression toward worse results. Tactical analysts use shot maps filtered by xG value to identify where on the pitch a team is conceding dangerous chances, and against which types of attack (crosses, cutbacks, through-balls).

### Goalkeeping Evaluation

Post-shot xG enables the most principled comparison of goalkeeper performance. Goals Saved Above Average (GSAA), calculated as PSxG faced minus goals conceded, controls for both the volume and quality of shots faced, making it possible to fairly compare goalkeepers playing behind different defensive structures. StatsBomb (2018) and Wyscout both provide PSxG; Stats Perform offers an equivalent metric called xGOT (Expected Goals on Target).

### Broadcasting and Fan Engagement

xG now appears regularly in live match broadcasts across the UK, Europe, and beyond. Sky Sports, the BBC, and Amazon Prime Video have all incorporated xG graphics into match coverage. Social media platforms surface xG as a common shorthand for "who deserved to win" (Stats Perform, 2025). This has driven significant growth in football data literacy among the general fan base, though it has also generated substantial misinterpretation.

### Betting Markets

xG models are used by betting operators to calibrate match odds and player proposition markets (Stats Perform, 2025). Some operators display live xG accumulations during matches to inform in-play betting. Research using xG-derived match probabilities against Bundesliga data found a simulated return on investment of approximately 10% against average market odds, rising to 15% at best available prices — though with substantial seasonal variation (Taber & Edwards, 2024).

---

## 8. Critiques & Limitations

### The Average Player Problem

The most fundamental conceptual limitation is that standard xG models assume the shooter is an average professional. A shot assigned 0.15 xG from 22 yards means an average player scores it 15% of the time; Erling Haaland may score it at 25%, and a central defender may score it at 8%. The model cannot distinguish these cases without player-specific adjustments. Critics — including professional coaches who reject xG precisely because it erases individual quality — are not wrong on this point; they are often, however, using it to dismiss the metric entirely rather than to demand a more nuanced version.

Defenders of xG respond that for the purpose of evaluating *chance quality* (how dangerous was the situation?) rather than *execution quality* (how well did the player shoot?), the average-player assumption is appropriate. The two are different questions and should be modelled separately (Davis & Robberechts, 2024).

### Sample Size and Within-Season Instability

Goals are rare events. A team takes roughly 400–500 shots per season across 38 matches. This is enough data for team-level xGD to carry real signal, but at the match level — roughly 10–15 shots per side — xG totals carry enormous variance. The literature broadly supports using xG as a seasonal or multi-match tool and discourages single-match inference (Rathke, 2017; Analytics FC, 2025).

At the individual player level, the sample size problem is more acute. A striker with 100 shots per season has enough data for xG per 90 to be reliable, but not for finishing quality (Goals/xG) to be stable — the persistence factor of approximately 0.1 per 60 shots implies most single-season finishing variance is noise (Analytics FC, 2025).

### Provider Inconsistency

Because different providers use different feature sets and training data, xG values for the same shot can differ materially across sources. This complicates cross-provider comparisons and makes it important to specify the model being used when reporting xG values (Taber & Edwards, 2024). The absence of a standardised, auditable xG specification is a genuine limitation of the field.

### The Non-Shot Blind Spot

Standard xG assigns zero expected value to dangerous situations that do not produce a shot — blocked runs, scrambled clearances, last-ditch tackles on through-balls. This systematically undervalues pressure and undervalues defensive actions. Possession-value frameworks address this gap but are a different model family entirely.

### Goalkeeper Model Design Issues

PSxG for goalkeeper evaluation requires excluding goalkeeper positioning from the model inputs by design (since positioning is itself the skill being measured). But this creates calibration challenges: a goalkeeper who consistently positions well will face shots with lower measured PSxG but actually lower difficulty, making their GSAA appear artificially high in some formulations (StatsBomb blog archive, 2018). Season-to-season PSxG-based GSAA is reported to vary by ±0.2 goals per 90 — a large window that counsels caution when drawing strong individual inferences (Willis, 2023).

### Contextual Factors Not Captured

Current event-data xG models do not incorporate fatigue, psychological pressure, pitch conditions, wind, or crowd noise — factors that influence shot conversion rates in ways that may be systematic rather than random. Pratas et al. (2023) experimented with including match attendance, game state (goal differential), and team/player value as features and found these variables ranked among the most important predictors in some Bundesliga models, challenging the assumption that shot context can be captured by spatial features alone.

---

## 9. Future Directions

### Tracking Data at Scale

The most significant frontier is the incorporation of full positional data — all 22 players and the ball, at 25 frames per second — into xG models. Anzer & Bauer (2021) demonstrated substantial accuracy improvements from positional data in the Bundesliga. As tracking data becomes available for more competitions through computer vision systems (reducing the cost premium of optical tracking), location-enriched models may become the standard rather than the exception.

### Sequence-Aware Models

The next methodological generation is likely to move beyond single-shot models to frameworks that assign credit across an entire attacking sequence. Joint modelling of shot creation probability (xS) and conversion probability (xG) — as proposed in the 2025 arxiv preprint — is one direction. Approaches that apply value functions over event sequences (related to xT and EPV frameworks) are another (Fernández et al., 2021).

### Player-Adjusted xG as Standard

The finishing skill literature has reached the point where player-adjusted xG — models with player random effects or separate per-position models — are methodologically well-supported. The practical question is whether data providers will publish these as standard products. StatsBomb's PSxG and Opta's xGOT are steps in this direction, but a fully player-adjusted pre-shot xG remains, as of this writing, largely an academic product.

### Women's Football and Lower Leagues

Most published xG models were trained primarily on data from elite men's competitions. Hudl StatsBomb has announced gender-aware model adjustments for the women's game (StatsBomb, 2025). Research on whether models transfer well across competition levels — and what features require re-calibration for lower leagues, different pitch dimensions, or youth football — remains underdeveloped.

### Explainability and Communication

A promising recent direction is making xG models more accessible to coaching staff through automated natural language explanation. Sumpter et al. (2024) demonstrated that logistic regression coefficients can be mapped to sentences describing shot quality factors, and that LLMs can then render these as plain-language shot descriptions — bridging the gap between model output and coaching comprehension. This is likely to be an important practical direction as clubs seek to operationalise model insights across technical departments with varying data literacy.

---

## 10. Conclusion

Expected Goals has earned its position at the centre of football analytics through a combination of theoretical soundness, empirical validation, and practical utility. Its core claim — that chance quality, not chance outcomes, is a more reliable signal of team and player performance — is well-supported across a large body of research and has been validated by the real-world success of clubs and analysts who adopted it early. Over a full season, xGD is the single best available predictor of both past performance quality and future results.

That said, xG is a model output, not a measurement. It carries the assumptions of its training data (average players, stationarity), the limitations of its inputs (event data misses defensive context), and the noise inherent in low-scoring sports with small shot samples per unit of analysis. Single-match xG should be treated as a starting point for analysis, not a verdict. Individual-level finishing inferences require large samples and Bayesian caution.

The metric is mature enough to build on. For the practitioner building PitchSights, xG is the correct foundation: it is the first-order signal that all downstream recruitment, tactical, and performance tools should anchor to. The frontier work — sequence-aware models, player adjustment, tracking enrichment, and PSxG-based goalkeeper evaluation — represents the roadmap for more precise implementations. But the central idea, now sixty-plus years in the making, is sound.

---

## 11. References

Analytics FC. (2025). *Are some players consistently good finishers?* https://analyticsfc.co.uk/blog/2025/02/11/are-some-players-consistently-good-finishers/

Anzer, G., & Bauer, P. (2021). A goal scoring probability model for shots based on synchronized positional and event data in football (soccer). *Frontiers in Sports and Active Living, 3*, 624475. https://doi.org/10.3389/fspor.2021.624475

Bandara, I., Shelyag, S., Rajasegarar, S., Dwyer, D., Kim, E., & Angelova, M. (2024). Predicting goal probabilities with improved xG models using event sequences in association football. *PLOS ONE*. https://pmc.ncbi.nlm.nih.gov/articles/PMC11524524/

Davis, J., & Robberechts, P. (2024). Biases in expected goals models confound finishing ability. *arXiv preprint*. https://arxiv.org/pdf/2401.09940

Ensum, J., Pollard, R., & Taylor, S. (2004). Applications of logistic regression to shots at goal in association football: Calculation of shot probabilities, quantification of factors and player/team. *Journal of Sports Sciences, 22*(6), 500–520. https://doi.org/10.1080/02640410410001730548

Fernández, J., Bornn, L., & Cervone, D. (2021). A framework for the fine-grained evaluation of the instantaneous expected value of soccer possessions. *Machine Learning, 110*, 1389–1427. https://doi.org/10.1007/s10994-021-05989-6

Hudl StatsBomb. (2025). *What are expected goals (xG)?* https://www.hudl.com/blog/expected-goals-xg-explained

Pollard, R., & Reep, C. (1997). Measuring the effectiveness of playing strategies at soccer. *Journal of the Royal Statistical Society: Series D (The Statistician), 46*(4), 541–550. https://doi.org/10.1111/1467-9884.00108

Pratas, J. M., Vaz, L., Valongo, B., & Briza, T. (2023). Expected goals in football: Improving model performance and demonstrating value. *PLOS ONE*. https://doi.org/10.1371/journal.pone.0282295

Rathke, A. (2017). An examination of expected goals and shot efficiency in soccer. *Journal of Human Sport and Exercise, 12*(Proc2), S514–S529. https://doi.org/10.14198/jhse.2017.12.Proc2.05

Reep, C., Pollard, R., & Benjamin, B. (1971). Skill and chance in ball games. *Journal of the Royal Statistical Society: Series A (General), 134*(4), 623–629. https://www.jstor.org/stable/2343657

Robberechts, P., & Davis, J. (2020). How data availability affects the ability to learn good xG models. In *Proceedings of the 2020 Machine Learning and Data Mining for Sports Analytics Workshop*. https://doi.org/10.1007/978-3-030-64912-8_14

Sæbø, O. D., & Hvattum, L. M. (2019). Evaluating the efficiency of association football players using a counting model. In *Statistical Modelling in Biostatistics and Bioinformatics*. Springer. [Referenced via StatsBomb blog archive]

StatsBomb Blog Archive. (2018, November). *A new way to measure keepers' shot stopping: Post-shot expected goals*. https://blogarchive.statsbomb.com/articles/soccer/a-new-way-to-measure-keepers-shot-stopping-post-shot-expected-goals/

StatsBomb Blog Archive. (2019). *Quantifying finishing skill*. https://blogarchive.statsbomb.com/articles/soccer/quantifying-finishing-skill/

Stats Perform. (2025). *Expected goals (xG): The football metric changing analysis, betting, and fan engagement*. https://www.statsperform.com/resource/expected-goals-xg-the-football-metric-changing-analysis-betting-and-fan-engagement/

Sumpter, D., et al. (2024). Automated explanation of machine learning models of footballing actions in words. *ResearchGate*. https://www.researchgate.net/publication/319420929_An_examination_of_expected_goals_and_shot_efficiency_in_soccer

Taber, J., & Edwards, R. (2024). Comparative analysis of expected goals models: Evaluating predictive accuracy and feature importance in European soccer. *ResearchGate*. https://www.researchgate.net/publication/387250442_Comparative_Analysis_of_Expected_Goals_Models_Evaluating_Predictive_Accuracy_and_Feature_Importance_in_European_Soccer

These Football Times. (2020, April 8). *The roots of expected goals (xG) and its journey from "nerd nonsense" to the mainstream*. https://thesefootballtimes.co/2020/04/08/the-roots-of-expected-goals-xg-and-its-journey-from-nerd-nonsense-to-the-mainstream/

Tippett, J. (2019). *The expected goals philosophy: A game-changing way of analysing football*. Hamilcar Publications.

Willis, S. (2023). *Advanced stats 101*. Cannon Stats. https://www.cannonstats.com/p/advanced-stats-101

Wikipedia. (2026). *Expected goals*. https://en.wikipedia.org/wiki/Expected_goals

---

*This is an AI-prepared review as part of the PitchSights Analytics Syllabus, a structured literature-first approach to building football analytics tools. Reviews are written for a technically literate audience and are intended to inform original implementation work, not substitute for it. AI has been known to hallucinate and as such, this is not intended to be used in any official publishing manner.*