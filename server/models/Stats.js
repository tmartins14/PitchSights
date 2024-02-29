const mongoose = require("mongoose");
const { Schema } = mongoose;

const statsSchema = new Schema(
  {
    match: { type: Schema.Types.ObjectId, ref: "matches", required: false }, // For match-specific stats
    player: { type: Schema.Types.ObjectId, ref: "players", required: false }, // For player-specific stats
    team: { type: Schema.Types.ObjectId, ref: "teams", required: false }, // For team-specific stats
    goals: Number,
    assists: Number,
    xG: Number,
    xA: Number,
    shots: Number,
    shotsOnTarget: Number,
    passes: Number,
    tackles: Number,
    interceptions: Number,
    clearances: Number,
    // Additional stats as needed
  },
  { timestamps: true }
);

mongoose.model("stats", statsSchema);
