const axios = require("axios");
const Player = require("../../../models/Player");
const fetchAllSeasonIds = require("../read/fetchAllSeasonIds");
const delay = require("../../delay");
const compareApiAndDbData = require("../../compareApiAndDbData");
const keys = require("../../../config/keys");
const { sportRadarAPI } = keys;

const updatePlayerData = async (playerData) => {
  try {
    const existingPlayer = await Player.findOne({
      where: { player_id: playerData.id.split(":")[2] },
    });

    const updateData = {
      player_id: playerData.id.split(":")[2],
      player_name: playerData.name,
      nationality: playerData.nationality,
      jersey_number: playerData.jersey_number,
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

    console.log(`Updated Player ${playerData.name}`);
  } catch (error) {
    console.log(`Error updating player ${playerData.name}: `, error);
  }
};

const fetchAndUpdatePlayers = async () => {
  const seasons = await fetchAllSeasonIds();

  for (season of seasons) {
    await delay(sportRadarAPI.accessLevel);

    const playerData = await axios.get(
      `${sportRadarAPI.URL}/${sportRadarAPI.accessLevel}/${sportRadarAPI.version}/${sportRadarAPI.languageCode}/seasons/${season}/players.json`,
      { params: { api_key: sportRadarAPI.soccerKey } }
    );

    // console.log(playerData.data.season_players);
    const playersArray = playerData.data.season_players.map(
      (player) => player.id
    );

    for (let i = 0; i < playersArray.length; i++) {
      await delay(sportRadarAPI.accessLevel);
      const playerProfile = await axios.get(
        `${sportRadarAPI.URL}/${sportRadarAPI.accessLevel}/${sportRadarAPI.version}/${sportRadarAPI.languageCode}/players/${playersArray[i]}/profile.json`,
        { params: { api_key: sportRadarAPI.soccerKey } }
      );

      playerData.data.season_players[i].nationality =
        playerProfile.data.player.nationality;

      // console.log(playerData.data.season_players[i]);
      await updatePlayerData(playerData.data.season_players[i]);
    }
  }
};

fetchAndUpdatePlayers()
  .then(() => console.log("All players have been updated"))
  .catch((error) =>
    console.log("An error occurred while updating player: ", error)
  )
  .finally();
