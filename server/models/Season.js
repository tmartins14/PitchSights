const mongoose = require("mongoose");
const { Schema } = mongoose;

const seasonSchema = new Schema(
  {
    year: String,
    league: { type: Schema.Types.ObjectId, ref: "leagues" },
    teams: [{ type: Schema.Types.ObjectId, ref: "teams" }],
    matches: [{ type: Schema.Types.ObjectId, ref: "matches" }],
  },
  { timestamps: true }
);

mongoose.model("seasons", seasonSchema);
