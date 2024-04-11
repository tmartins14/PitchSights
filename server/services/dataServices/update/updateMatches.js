const axios = require("axios");
const Match = require("../../../models/Match.js");
const MetaData = require("../../../models/MetaData"); // Sequelize model for MetaData
const fetchMetaData = require("../read/fetchMetaData");
const delay = require("../../delay");
const compareApiAndDbData = require("../../compareApiAndDbData");
const keys = require("../../../config/keys");
const { apiFootball } = keys;

const updateMatchData = async (matchData, season) => {
  try {
    // Check for existing match
    const existingMatch = await Match.findOne({
      where: { fixture_id: matchData.fixture.id },
    });

    const updateData = {
      fixture_id: matchData.fixture.id,
      league_id: matchData.league.id,
      season_year: matchData.league.season,
      home_team_id: matchData.teams.home.id,
      away_team_id: matchData.teams.away.id,
      winner_id: matchData.teams.home.winner
        ? matchData.teams.home.id
        : matchData.teams.away.winner
        ? matchData.teams.away.id
        : null,
      home_score: matchData.goals.home,
      away_score: matchData.goals.away,
      start_datetime: matchData.fixture.date,
      venue: matchData.fixture.venue.name,
    };

    if (!existingMatch) {
      await Match.create(updateData);
    } else {
      if (!compareApiAndDbData(existingMatch, updateData)) {
        await Match.update(updateData, {
          where: { fixture_id: updateData.fixture_id },
        });
      }
    }

    // Update meta data to track which matches have been updated - required due to API service limitation
    await MetaData.update(
      { matches_updated: true },
      { where: { season_id: season.season_id } }
    );

    console.log(`Updated Match ${matchData.fixture.id}`);
  } catch (error) {
    console.log(`Error update match: ${matchData.fixture.id}: `, error);
  }
};

const fetchAndUpdateMatches = async () => {
  const updateSeasons = await fetchMetaData("matches");

  // API limits requests per minute
  let counter = 0;

  for (season of updateSeasons) {
    const config = {
      method: "get",
      url: `https://v3.football.api-sports.io/fixtures?league=${season.league_id}&season=${season.season_year}`,
      headers: {
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": apiFootball.apiKey,
      },
    };

    if (counter === apiFootball.rateLimit) {
      await delay();
      counter = 0;
    }

    const matchData = await axios(config);

    for (match of matchData.data.response) {
      updateMatchData(match, season);
    }

    counter++;
  }
};

fetchAndUpdateMatches()
  .then(() => console.log("All matches have been updated"))
  .catch((error) =>
    console.log("An error occurred while updating matches: ", error)
  );
