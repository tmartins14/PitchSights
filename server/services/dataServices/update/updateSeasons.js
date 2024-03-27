const axios = require("axios");
const Season = require("../../../models/Season"); // Sequelize model for Seasons
const keys = require("../../../config/keys");
const { apiFootball } = keys;

const leagues = require("../../../data/leaguesList");

const updateSeasonData = async (seasonData) => {
  try {
    // Use upsert to either create a new record or update an existing one
    await Season.upsert(seasonData);

    console.log(`Updated Season: ${seasonData.season_name}`);
  } catch (error) {
    console.error(`Error updating season: ${seasonData.season_name}`, error);
  }
};

const fetchAndUpdateSeasons = async () => {
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
  const seasonData = [];
  try {
    const apiResponse = await axios(config);

    apiResponse.data.response.forEach((el) => {
      if (leagueIds.has(el.league.id)) {
        leagueData.push(el);
      }
    });

    for (league of leagueData) {
      let seasonObj = {
        league_id: league.league.id,
        season_name: `${league.league.name}`,
      };

      for (season of league.seasons) {
        seasonObj.season_name += ` - ${season.year}`;
        seasonObj.start_date = season.start;
        seasonObj.end_date = season.end;

        seasonData.push(seasonObj);

        // Reset object
        seasonObj = {
          league_id: league.league.id,
          season_name: `${league.league.name}`,
        };
      }
    }

    for (const season of seasonData) {
      await updateSeasonData(season);
    }
  } catch (error) {
    console.error("An error occurred while updating seasons:", error);
  }
};

fetchAndUpdateSeasons();
