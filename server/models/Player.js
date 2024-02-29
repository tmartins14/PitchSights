const mongoose = require("mongoose");
const { Schema } = mongoose;

const playerSchema = new Schema({
  team: {
    type: Schema.Types.ObjectId,
    ref: "teams",
  },
  playerName: String,
  position: String,
  nationality: String,
});

mongoose.model("players", playerSchema);
