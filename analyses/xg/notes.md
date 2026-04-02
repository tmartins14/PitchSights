# Literature Review Notes

## 4. Theoretical Foundation
- Standard model seems but lacks the real-world appicability one would see in a match. I.e position of keeper, number of defenders blocking the goal, pressure on the shooter, etc.
- When we see xG data from Understat or Opta, which model are we seeing?
- How much impact does the advanced features have on the model?
- Assumptions:
    -  Independence: Every shot is independent of one another but that is not necessarily the case for shots in the same match. Fatigue, keeper confidence, and momentum can all impact future shot quality. *Do advanced xG models account for this in game state features?*
    - Average Player: This makes sense. Can the player's finishing ability be used as a feature to adjust the xG up or down? Is that what positional and player adjusted extensions do?
    - Stationarity: Cross-league validity is not perfect and neither is xG. Not much can be done here (I don't think).

## 5. Key Studies & Empirical Findings

**Predictive Validity of xG at the Team Level**
- xG and xGD is better at predicting long term performance and league rankings than individual match outcomes.

**Finishing Skill and the xG Overperformance Question**
- It seems like Goals compared to xG is not a good measure of finishing ability
- Post-Shot xG (PSxG) seems like a better measure for finishing quality. Also a good measure for goal keeper quality.


## 6. Methodlogical Variations & Debates

**Event-Data vs Tracking-Data Models**
- Event Data: Inferior in theory, superior in data availability
- Tracking Data: Superior in theory, inferior in data availability

**The Non-Shot Problem**
- It seems like a lot of teams play for xG and not xS or xT, EPV, OBV, etc. 

## 7. Practical Applications

**Betting Markets**
- Every book maker uses xG. What *aren't* they using?

## 8. Critiques & Limitations

**The Average Player Problem**
- Yes, Haaland will score from a particular situation at a high probability than John Stones. But that's not the point. xG measures *chance quality not shot quality*.


## Thoughts
- An xG model can be the best in the world but if you can't get someone to use it (i.e. a coach), it's useless. Logistic models, although inferior in predictive power, offer explainability. **Communication of this data is key**.

## Questions
- It seems like more teams are trying to play into "high xG" areas, akin to shooting at the 3-point line in basketball. But this leads to the "Low Block" defensive tactic which makes the number of high xG shots much lower or lowers the xG (more players in front of the ball). Why are we not seeing more outide-the-box shots (aka the "screamers") more? Would this game theory approach not be similar to NFL football game of Run/Pass offense vs defense? "If you stop the run, I'll throw the ball and vice-versa"
- What assumptions go into xG maps? How can this be tweaked based on the different features?