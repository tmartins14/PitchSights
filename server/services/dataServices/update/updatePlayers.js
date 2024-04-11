const axios = require("axios");
const Player = require("../../../models/Player.js");
const MetaData = require("../../../models/MetaData"); // Sequelize model for MetaData
const fetchMetaData = require("../read/fetchMetaData");
const delay = require("../../delay");
const compareApiAndDbData = require("../../compareApiAndDbData");
const keys = require("../../../config/keys");
const { apiFootball } = keys;

const updatePlayerData = async (playerData, season) => {
  try {
    // Check for existing match
    const existingPlayer = await Player.findOne({
      where: { player_id: playerData.player.id },
    });

    const updateData = {
      player_id: playerData.player.id,
      player_name: playerData.player.name,
      nationality: playerData.player.nationality,
      age: playerData.player.age,
    };

    if (!existingPlayer) {
      await Player.create(updateData);
    } else {
      if (!compareApiAndDbData(existingPlayer, updateData)) {
        await Player.update(updateData, {
          where: { player_id: updateData.player_id },
        });
      }
    }

    // Update meta data to track which matches have been updated - required due to API service limitation
    await MetaData.update(
      { players_updated: true },
      { where: { season_id: season.season_id } }
    );

    console.log(`Updated Player ${playerData.player.id}`);
  } catch (error) {
    console.log(`Error update player: ${playerData.player.id}: `, error);
  }
};

const fetchAndUpdatePlayers = async () => {
  const updateSeasons = await fetchMetaData("players");

  // API limits requests per minute
  let counter = 0;

  for (season of updateSeasons) {
    const config = {
      method: "get",
      url: `https://v3.football.api-sports.io/players?league=${season.league_id}&season=${season.season_year}`,
      headers: {
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": apiFootball.apiKey,
      },
    };

    if (counter === apiFootball.rateLimit) {
      await delay();
      counter = 0;
    }

    const playerData = await axios(config);

    for (player of playerData.data.response) {
      console.log(player);
      updatePlayerData(player, season);
    }

    counter++;
  }
};

fetchAndUpdatePlayers()
  .then(() => console.log("All players have been updated"))
  .catch((error) =>
    console.log("An error occurred while updating players: ", error)
  );
