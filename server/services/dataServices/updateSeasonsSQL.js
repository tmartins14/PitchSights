const axios = require("axios");
const database = require("../database"); // Adjust the path as necessary
const Season = require("../../models/Season"); // Sequelize model for Seasons
const League = require("../../models/League"); // Sequelize model for Leagues
const keys = require("../../config/keys");
const { sportRadarAPI } = keys;

const leagues = require("../../data/leagues");

// Assuming database connection is established in your database.js via Sequelize

const leaguesArray = leagues.competitions.map((league) => league.id);

const updateSeasonData = async (seasonData) => {
  try {
    const updateData = {
      seasonId: seasonData.id.split(":")[2],
      name: seasonData.name,
      leagueId: seasonData.competition_id.split(":")[2],
      startDate: seasonData.start_date,
      endDate: seasonData.end_date,
    };

    // Use upsert to either create a new record or update an existing one
    await Season.upsert(updateData);

    console.log(`Updated Season: ${seasonData.name}`);
  } catch (error) {
    console.error(`Error updating season: ${seasonData.name}`, error);
  }
};

const fetchAndUpdateSeasons = async () => {
  try {
    const seasonData = await axios.get(
      `${sportRadarAPI.URL}/${sportRadarAPI.accessLevel}/${sportRadarAPI.version}/${sportRadarAPI.languageCode}/seasons.json`,
      { params: { api_key: sportRadarAPI.soccerKey } }
    );

    const filteredSeasonData = seasonData.data.seasons.filter(
      (season) => leaguesArray.includes(season.competition_id) // Adjust based on the actual structure of your competition_id
    );

    for (const season of filteredSeasonData) {
      await updateSeasonData(season);
    }

    console.log("All seasons have been updated");
  } catch (error) {
    console.error("An error occurred while updating seasons:", error);
  }
};

fetchAndUpdateSeasons();
