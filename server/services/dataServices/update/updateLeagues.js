const axios = require("axios");
const League = require("../../../models/League"); // Ensure this path is correct
const keys = require("../../../config/keys");
const { apiFootball } = keys;

const leagues = require("../../../data/leaguesList");

// MySQL Connection using Sequelize is already handled in your database.js

const updateLeagueData = async (leagueData) => {
  try {
    // Check for existing league by leagueId
    const existingLeague = await League.findOne({
      where: { league_id: leagueData.league.id },
    });

    const updateData = {
      league_id: leagueData.league.id,
      league_name: leagueData.league.name,
      country: leagueData.country.name,
      league_logo: leagueData.league.logo,
    };

    if (!existingLeague) {
      await League.create(updateData);
    } else {
      if (!compareApiAndDbData(existingLeague, updateData)) {
        await League.update(updateData, {
          where: { league_id: updateData.league_id },
        });
      }
    }

    console.log(`Updated League: ${leagueData.name}`);
  } catch (error) {
    console.error(`Error updating league: ${leagueData.name}`, error);
  }
};

const fetchAndUpdateLeagues = async () => {
  const config = {
    method: "get",
    url: "https://v3.football.api-sports.io/leagues",
    headers: {
      "x-rapidapi-host": "v3.football.api-sports.io",
      "x-rapidapi-key": apiFootball.apiKey,
    },
  };

  const leagueIds = new Set(Object.values(leagues));
  const leagueData = [];

  try {
    const apiResponse = await axios(config);

    apiResponse.data.response.forEach((el) => {
      if (leagueIds.has(el.league.id)) {
        leagueData.push(el);
      }
    });

    for (const league of leagueData) {
      await updateLeagueData(league);
    }
  } catch (error) {
    console.log(`An error occurred updating  leagues: ${error}`);
  }
};

fetchAndUpdateLeagues()
  .then(() => console.log("All leagues have been updated"))
  .catch((error) =>
    console.log("An error occurred while updating leagues: ", error)
  );
