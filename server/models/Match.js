const mongoose = require("mongoose");
const { Schema } = mongoose;

const matchSchema = new Schema(
  {
    matchId: { type: String, required: true },
    homeTeam: { type: Schema.Types.ObjectId, ref: "teams" },
    awayTeam: { type: Schema.Types.ObjectId, ref: "teams" },
    startTime: { type: Date, required: true },
    date: Date,
    league: { type: Schema.Types.ObjectId, ref: "leagues" },
    season: { type: Schema.Types.ObjectId, ref: "seasons" },
    matchStatus: {
      type: String,
      enum: ["Not Started", "In Progress", "Finished"],
    },
    homeScore: { type: Number, required: true },
    awayScore: { type: Number, required: true },
    matchWinner: { type: Schema.Types.ObjectId, ref: "teams" },
    //     stats: { type: Schema.Types.ObjectId, ref: "stats" }, // Match-level statistics
    //     events: [{ type: Schema.Types.ObjectId, ref: "events" }], // All events occurring in the match
  },
  { timestamps: true }
);

const Match = mongoose.model("matches", matchSchema);

module.exports = Match;
