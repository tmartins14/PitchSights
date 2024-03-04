const axios = require("axios");
const mongoose = require("mongoose");
const Season = require("../../models/Season.js");
const League = require("../../models/League.js");
const keys = require("../../config/keys");
const { sportRadarAPI } = keys;

const leagues = require("../../data/leagues");

// MongoDB Connection
mongoose.connect(keys.mongoURI);

// Import Leagues that will be stored in DB
const leaguesArray = leagues.competitions.map((league) => league.id);

// Get the League Object ID by the League ID
const findLeagueById = async (leagueId) => {
  const league = await League.findOne({ leagueId: leagueId });
  return league ? league._id : null;
};

const updateSeasonData = async (seasonData) => {
  // Will check if season already exists
  const existingSeason = await Season.findOne({ seasonId: seasonData.id });

  // Need the league's database id to connect it to a specific season
  const leagueObjectId = await findLeagueById(seasonData.competition_id);

  try {
    const updateData = {
      seasonId: seasonData.id,
      seasonName: seasonData.name,
      league: leagueObjectId,
      year: seasonData.year,
      startDate: seasonData.start_date,
      endDate: seasonData.end_date,
    };

    if (!existingSeason) {
      await new Season(updateData).save();
    } else {
      const isDifferent =
        existingSeason.seasonName !== updateData.seasonName ||
        existingSeason.year !== updateData.year ||
        existingSeason.startDate !== updateData.startDate ||
        existingSeason.end_date !== updateData.end_date;

      if (isDifferent) {
        await Season.updateOne({ seasonId: updateData.seasonId });
      }
    }

    console.log(`Updated Season: ${seasonData.name}`);
  } catch (error) {
    console.error(`Error updating league: ${seasonData.name}`, error);
  }
};

const fetchAndUpdateSeason = async () => {
  const seasonData = await axios.get(
    `${sportRadarAPI.URL}/${sportRadarAPI.accessLevel}/${sportRadarAPI.version}/${sportRadarAPI.languageCode}/seasons.json`,
    { params: { api_key: sportRadarAPI.soccerKey } }
  );

  const filteredSeasonData = seasonData.data.seasons.filter((season) =>
    leaguesArray.includes(season.competition_id)
  );

  for (const season of filteredSeasonData) {
    await updateSeasonData(season);
  }

  //   console.log(filteredSeasonData);
};

fetchAndUpdateSeason()
  .then(() => console.log("All seasons have been updated"))
  .catch((error) =>
    console.log("An error occurred while updating seasons: ", error)
  )
  .finally(() => mongoose.disconnect());
