const axios = require("axios");
const keys = require("../../config/keys");
const { apiFootball } = keys;

const fetchMatchStats = async (matchId) => {
  try {
    const config = {
      method: "get",
      url: `https://v3.football.api-sports.io/fixtures/statistics?fixture=${matchId}`,
      headers: {
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": apiFootball.apiKey,
      },
    };

    const matchStats = await axios(config);

    return matchStats.data.response;
  } catch (error) {
    console.log("Error fetching match events", error);
  }
};

// Wrapping the call in an async function
async function main() {
  const matchStats = await fetchMatchStats(1035481);
  console.log(matchStats);
}

main(); // Call the async function

module.exports = fetchMatchStats;
