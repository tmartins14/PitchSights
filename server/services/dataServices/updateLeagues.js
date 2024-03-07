const axios = require("axios");
const Sequelize = require("sequelize");
const database = require("../database"); // Adjust the path as necessary
const League = require("../../models/league"); // Ensure this path is correct
const keys = require("../../config/keys");
const { sportRadarAPI } = keys;

const leagues = require("../../data/leagues");

// MySQL Connection using Sequelize is already handled in your database.js

// Define leaguesArray based on your JSON structure
const leaguesArray = leagues.competitions.map((league) => league.id);

console.log(leaguesArray);

const updateLeagueData = async (leagueData) => {
  try {
    // Check for existing league by leagueId
    const existingLeague = await League.findOne({
      where: { leagueId: leagueData.id },
    });

    console.log(existingLeague);
    const updateData = {
      leagueId: leagueData.id.split(":")[2],
      leagueName: leagueData.name,
      country: leagueData.category.name,
      gender: leagueData.gender,
    };

    if (!existingLeague) {
      // If league does not exist, create a new entry
      await League.create(updateData);
    } else {
      // If the league exists and has different data, update it
      const isDifferent =
        existingLeague.leagueName !== updateData.leagueName ||
        existingLeague.country !== updateData.country ||
        existingLeague.gender !== updateData.gender;

      if (isDifferent) {
        await League.update(updateData, {
          where: { leagueId: updateData.leagueId },
        });
      }
    }

    console.log(`Updated League: ${leagueData.name}`);
  } catch (error) {
    console.error(`Error updating league: ${leagueData.name}`, error);
  }
};

const fetchAndUpdateLeagues = async () => {
  const leagueData = await axios.get(
    `${sportRadarAPI.URL}/${sportRadarAPI.accessLevel}/${sportRadarAPI.version}/${sportRadarAPI.languageCode}/competitions.json`,
    { params: { api_key: sportRadarAPI.soccerKey } }
  );

  const filteredLeagueData = leagueData.data.competitions.filter((league) =>
    leaguesArray.includes(league.id)
  );

  console.log("Filtered Leagues:", filteredLeagueData);

  for (const league of filteredLeagueData) {
    await updateLeagueData(league);
  }
};

fetchAndUpdateLeagues()
  .then(() => console.log("All leagues have been updated"))
  .catch((error) =>
    console.log("An error occurred while updating leagues: ", error)
  );
