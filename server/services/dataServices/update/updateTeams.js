const axios = require("axios");
const Team = require("../../../models/Team");
const MetaData = require("../../../models/MetaData"); // Sequelize model for MetaData
const fetchMetaData = require("../read/fetchMetaData");
const delay = require("../../delay");
const compareApiAndDbData = require("../../compareApiAndDbData");
const keys = require("../../../config/keys");
const { apiFootball } = keys;

const updateTeamData = async (teamData, season) => {
  try {
    const existingTeam = await Team.findOne({
      where: { team_id: teamData.id },
    });

    const updateData = {
      team_id: teamData.id,
      name: teamData.name,
      abbv: teamData.code,
      logo: teamData.logo,
    };

    if (!existingTeam) {
      await Team.create(updateData);
    } else {
      if (!compareApiAndDbData(existingTeam, updateData)) {
        await Team.update(updateData, {
          where: { team_id: updateData.team_id },
        });
      }
    }

    // Update meta data to track which teams have been updated - required due to API service limitation
    await MetaData.update(
      { teams_updated: true },
      { where: { season_id: season.season_id } }
    );
    console.log(`Updated Team ${teamData.name}`);
  } catch (error) {
    console.log(`Error updating team ${teamData.name}: `, error);
  }
};

const fetchAndUpdateTeams = async () => {
  const updateSeasons = await fetchMetaData("teams");

  // API limits requests per minute
  let counter = 0;
  // console.log(seasons);
  for (season of updateSeasons) {
    const config = {
      method: "get",
      url: `https://v3.football.api-sports.io/teams?league=${season.league_id}&season=${season.season_year}`,
      headers: {
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": apiFootball.apiKey,
      },
    };

    if (counter === apiFootball.rateLimit) {
      await delay();
      counter = 0;
    }
    const teamData = await axios(config);

    // console.log(teamData.data.response);

    for (team of teamData.data.response) {
      await updateTeamData(team.team, season);
    }

    counter++;
  }
};

fetchAndUpdateTeams()
  .then(() => console.log("All teams have been updated"))
  .catch((error) =>
    console.log("An error occurred while updating teams: ", error)
  );
