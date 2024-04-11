const axios = require("axios");
const keys = require("../../../config/keys");
const { apiFootball } = keys;

const fetchAllTeams = async (league, season) => {
  const config = {
    method: "get",
    url: `https://v3.football.api-sports.io/teams?league=${league}&season=${season}`,
    headers: {
      "x-rapidapi-host": "v3.football.api-sports.io",
      "x-rapidapi-key": apiFootball.apiKey,
    },
  };

  try {
    const apiResponse = await axios(config);

    console.log(apiResponse.data.response);
  } catch (error) {
    console.log("Error fetching seasons from database: ", error);
  }
};

fetchAllTeams(39, 2022);

// module.exports = fetchAllSeasonIds;
