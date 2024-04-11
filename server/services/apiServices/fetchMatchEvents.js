const axios = require("axios");
const keys = require("../../config/keys");
const { apiFootball } = keys;

const fetchMatchEvents = async (matchId) => {
  try {
    const config = {
      method: "get",
      url: `https://v3.football.api-sports.io/fixtures/events?fixture=${matchId}`,
      headers: {
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": apiFootball.apiKey,
      },
    };

    const matchEvents = await axios(config);

    return matchEvents.data.response;
  } catch (error) {
    console.log("Error fetching match events", error);
  }
};

module.exports = fetchMatchEvents;
