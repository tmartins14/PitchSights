const axios = require("axios");
const mongoose = require("mongoose");
const Season = require("../../models/Season.js");
const Match = require("../../models/Match.js");
const keys = require("../../config/keys");
const { sportRadarAPI } = keys;

// MongoDB Connection
mongoose.connect(keys.mongoURI);

// Function to get all seasons currently stored in Database
const getSeasons = async () => {
  try {
    const seasons = await Season.find({}).select("seasonId");

    const seasonIdArray = seasons.map((season) => season.seasonId);

    console.log(seasonIdArray);
    return seasonIdArray;
  } catch (error) {
    console.log("Error occurred while getting the season IDs: ", error);
  }
};

// Function to get all matches per Season ID
const getMatchesBySeasonId = async (seasonId) => {};

const updateMatchesInDB = async () => {};

getSeasons();
