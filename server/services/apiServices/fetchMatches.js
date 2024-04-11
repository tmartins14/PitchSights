const axios = require("axios");
const keys = require("../../config/keys");
const { apiFootball } = keys;

const fetchMatches = async (leagueId, season, date) => {
  try {
    const config = {
      method: "get",
      url: `https://v3.football.api-sports.io/fixtures?league=${leagueId}&season=${season}&date=${date}`,
      headers: {
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": apiFootball.apiKey,
      },
    };

    const matches = await axios(config);

    return matches.data.response;
  } catch (error) {
    console.log("Error fetching matches", error);
  }
};

// Wrapping the call in an async function
async function main() {
  const matches = await fetchMatches(39, 2023, "2024-04-02");
  console.log(matches);
}

main(); // Call the async function

module.exports = fetchMatches;
