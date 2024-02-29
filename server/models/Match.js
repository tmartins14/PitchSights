const mongoose = require("mongoose");
const { Schema } = mongoose;

const matchSchema = new Schema(
  {
    homeTeam: { type: Schema.Types.ObjectId, ref: "teams" },
    awayTeam: { type: Schema.Types.ObjectId, ref: "teams" },
    date: Date,
    league: { type: Schema.Types.ObjectId, ref: "leagues" },
    season: { type: Schema.Types.ObjectId, ref: "seasons" },
    stats: { type: Schema.Types.ObjectId, ref: "stats" }, // Match-level statistics
    events: [{ type: Schema.Types.ObjectId, ref: "events" }], // All events occurring in the match
  },
  { timestamps: true }
);

mongoose.model("matches", matchSchema);
