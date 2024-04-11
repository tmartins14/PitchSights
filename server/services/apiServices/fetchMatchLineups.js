const axios = require("axios");
const keys = require("../../config/keys");
const { apiFootball } = keys;

const fetchMatchLineups = async (matchId) => {
  try {
    const config = {
      method: "get",
      url: `https://v3.football.api-sports.io/fixtures/lineups?fixture=${matchId}`,
      headers: {
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": apiFootball.apiKey,
      },
    };

    const matchLineups = await axios(config);

    return matchLineups.data.response;
  } catch (error) {
    console.log("Error fetching match events", error);
  }
};

// Wrapping the call in an async function
async function main() {
  const matchLineups = await fetchMatchLineups(1035481);
  console.log(matchLineups);
}

main(); // Call the async function

module.exports = fetchMatchLineups;
