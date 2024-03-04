const mongoose = require("mongoose");
const { Schema } = mongoose;

const playerSchema = new Schema(
  {
    playerId: { type: String, required: true },
    team: {
      type: Schema.Types.ObjectId,
      ref: "teams",
    },
    playerName: String,
    position: String,
    nationality: String,
    stats: [{ type: Schema.Types.ObjectId, ref: "stats" }],
  },
  { timestamps: true }
);

const Player = mongoose.model("players", playerSchema);

module.exports = Player;
