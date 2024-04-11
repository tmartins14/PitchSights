const axios = require("axios");
const Season = require("../../../models/Season"); // Sequelize model for Seasons
const MetaData = require("../../../models/MetaData"); // Sequelize model for Seasons
const keys = require("../../../config/keys");
const { apiFootball } = keys;
const Sequelize = require("sequelize");

const leagues = require("../../../data/leaguesList");
const compareApiAndDbData = require("../../compareApiAndDbData");

const updateSeasonData = async (seasonData) => {
  try {
    const existingSeason = await Season.findOne({
      where: { season_id: seasonData.season_id },
    });

    if (!existingSeason) {
      await Season.create(seasonData);
    } else {
      if (!compareApiAndDbData(existingSeason, seasonData)) {
        await existingSeason.update(seasonData, {
          where: { season_id: seasonData.season_id },
        });
      }
    }

    // Update meta data table to track teams and matches that have been updated
    const metaDataUpdate = {
      season_id: seasonData.season_id,
      season_year: seasonData.season_year,
      league_id: seasonData.league_id,
      current: seasonData.current,
      teams_updated: false,
      matches_updated: false,
    };

    await MetaData.update(metaDataUpdate, {
      where: { season_id: metaDataUpdate.season_id },
    });

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
        seasonObj.season_id = `${season.year}${league.league.id}`;
        seasonObj.season_year = season.year;
        seasonObj.season_name += ` - ${season.year}`;
        seasonObj.start_date = season.start;
        seasonObj.end_date = season.end;
        seasonObj.current = season.current;

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
