const database = require("../services/database");

const League = require("./League");
const Season = require("./Season");
const Match = require("./Match");
const Team = require("./Team");
const Player = require("./Player");

module.exports = { database, models: { League, Season, Match, Team, Player } };
