const mongoose = require("mongoose");
const { Schema } = mongoose;

const leagueSchema = new Schema(
  {
    leagueId: { type: String, required: true },
    leagueName: String,
    country: String,
    gender: { type: String, required: true },
  },
  { timestamps: true }
);

const League = mongoose.model("leagues", leagueSchema);

module.exports = League;
