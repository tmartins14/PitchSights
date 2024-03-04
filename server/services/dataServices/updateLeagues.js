const axios = require("axios");
const mongoose = require("mongoose");
const League = require("../../models/League.js");
const keys = require("../../config/keys");
const { sportRadarAPI } = keys;

const leagues = require("../../data/leagues");

// MongoDB Connection
mongoose.connect(keys.mongoURI);

const URL = sportRadarAPI.URL;
const accessLevel = sportRadarAPI.accessLevel;
const version = sportRadarAPI.version;
const languageCode = sportRadarAPI.languageCode;
const soccerKey = sportRadarAPI.soccerKey;

const leaguesArray = leagues.competitions.map((league) => league.id);

const updateLeagueData = async (leagueData) => {
  const existingLeague = await League.findOne({ leagueId: leagueData.id });

  try {
    const updateData = {
      leagueId: leagueData.id,
      leagueName: leagueData.name,
      country: leagueData.category.name,
      gender: leagueData.gender,
    };

    if (!existingLeague) {
      await new League(updateData).save();
    } else {
      const isDifferent =
        existingLeague.leagueName !== updateData.leagueName ||
        existingLeague.country !== updateData.country ||
        existingLeague.gender !== updateData.gender;

      if (isDifferent) {
        await League.updateOne({ leagueId: updateData.leagueId });
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
    { params: { api_key: soccerKey } }
  );

  const filteredLeagueData = leagueData.data.competitions.filter((league) =>
    leaguesArray.includes(league.id)
  );

  for (const league of filteredLeagueData) {
    await updateLeagueData(league);
  }
};

fetchAndUpdateLeagues()
  .then(() => console.log("All leagues have been updated"))
  .catch((error) =>
    console.log("An error occurred while updating leagues: ", error)
  )
  .finally(() => mongoose.disconnect());
