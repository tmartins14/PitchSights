const axios = require("axios");
const Team = require("../../../models/Team");
const fetchAllSeasonIds = require("../read/fetchAllSeasonIds");
const delay = require("../../delay");
const compareApiAndDbData = require("../../compareApiAndDbData");
const keys = require("../../../config/keys");
const { sportRadarAPI } = keys;

const updateTeamData = async (teamData) => {
  try {
    const existingTeam = await Team.findOne({
      where: { competitor_id: teamData.id.split(":")[2] },
    });

    const updateData = {
      competitor_id: teamData.id.split(":")[2],
      full_name: teamData.name,
      short_name: teamData.short_name,
      abbv: teamData.abbreviation,
    };

    if (!existingTeam) {
      await Team.create(updateData);
    } else {
      if (!compareApiAndDbData(existingTeam, updateData)) {
        await Team.update(updateData, {
          where: { competitor_id: updateData.competitor_id },
        });
      }
    }
    console.log(`Updated Team ${teamData.name}`);
  } catch (error) {
    console.log(`Error updating team ${teamData.name}: `, error);
  }
};

const fetchAndUpdateTeams = async () => {
  const seasons = await fetchAllSeasonIds();

  for (season of seasons) {
    await delay(sportRadarAPI.accessLevel); // Delay to prevent hitting API too quickly

    const teamData = await axios.get(
      `${sportRadarAPI.URL}/${sportRadarAPI.accessLevel}/${sportRadarAPI.version}/${sportRadarAPI.languageCode}/seasons/${season}/competitors.json`,
      { params: { api_key: sportRadarAPI.soccerKey } }
    );

    for (team of teamData.data.season_competitors) {
      await updateTeamData(team);
    }
  }
};

fetchAndUpdateTeams()
  .then(() => console.log("All teams have been updated"))
  .catch((error) =>
    console.log("An error occurred while updating teams: ", error)
  );
