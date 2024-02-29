const mongoose = require("mongoose");
const { Schema } = mongoose;

const teamSchema = new Schema(
  {
    name: String,
    league: { type: Schema.Types.ObjectId, ref: "leagues" },
    season: { type: Schema.Types.ObjectId, ref: "seasons" },
    players: [{ type: Schema.Types.ObjectId, ref: "players" }],
    stats: { type: Schema.Types.ObjectId, ref: "stats" }, // Aggregated team stats, potentially by season
  },
  { timestamps: true }
);

mongoose.model("teams", teamSchema);
