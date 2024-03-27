const axios = require("axios");
const Match = require("../../../models/Match.js");
const keys = require("../../../config/keys");
const { sportRadarAPI } = keys;
const fetchAllSeasonIds = require("../read/fetchAllSeasonIds");
const delay = require("../../delay");
const compareApiAndDbData = require("../../compareApiAndDbData");

const updateMatchData = async ({ sport_event, sport_event_status }) => {
  try {
    // Check for existing match
    const existingMatch = await Match.findOne({
      where: { sport_event_id: sport_event.id.split(":")[2] },
    });

    const updateData = {
      sport_event_id: sport_event.id.split(":")[2],
      competition_id:
        sport_event.sport_event_context.competition.id.split(":")[2],
      season_id: sport_event.sport_event_context.season.id.split(":")[2],
      home_team_id: sport_event.competitors[0].id.split(":")[2],
      away_team_id: sport_event.competitors[1].id.split(":")[2],
      winner_id:
        sport_event_status && sport_event_status.winner_id
          ? sport_event_status.winner_id.split(":")[2]
          : null,
      home_score: sport_event_status.home_score,
      away_score: sport_event_status.away_score,
      start_time: sport_event.start_time,
      venue: sport_event && sport_event.venue ? sport_event.venue.name : null,
    };

    if (!existingMatch) {
      await Match.create(updateData);
    } else {
      if (!compareApiAndDbData(existingMatch, updateData)) {
        await Match.update(updateData, {
          where: { sport_event_id: updateData.sport_event_id },
        });
      }
    }
    console.log(`Updated Match ${sport_event.id}`);
  } catch (error) {
    console.log(`Error update match: ${sport_event.id}: `, error);
  }
};

const fetchAndUpdateMatches = async () => {
  const seasons = await fetchAllSeasonIds();

  for (season of seasons) {
    await delay(sportRadarAPI.accessLevel);

    const matchData = await axios.get(
      `${sportRadarAPI.URL}/${sportRadarAPI.accessLevel}/${sportRadarAPI.version}/${sportRadarAPI.languageCode}/seasons/${season}/schedules.json`,
      { params: { api_key: sportRadarAPI.soccerKey } }
    );

    for (match of matchData.data.schedules) {
      await updateMatchData(match);
    }
  }
};

fetchAndUpdateMatches()
  .then(() => console.log("All matches have been updated"))
  .catch((error) =>
    console.log("An error occurred while updating matches: ", error)
  );
