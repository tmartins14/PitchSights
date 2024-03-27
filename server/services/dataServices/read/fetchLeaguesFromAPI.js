const axios = require("axios");
const { apiFootball } = require("../../../config/dev");

const leagues = require("../../../data/leaguesList");

const fetchLeaguesFromAPI = async () => {
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
    const leaguesAPIResponse = await axios(config);

    leaguesAPIResponse.data.response.forEach((el) => {
      if (leagueIds.has(el.league.id)) {
        leagueData.push(el);
      }
    });

    console.log(leagueData);
    return leagueData;
  } catch (error) {
    console.log(error);
  }
};

fetchLeaguesFromAPI();
