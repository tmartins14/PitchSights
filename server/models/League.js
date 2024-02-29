const mongoose = require("mongoose");
const { Schema } = mongoose;

const leagueSchema = new Schema(
  {
    leagueName: String,
    country: String,
    seasons: [{ type: Schema.Types.ObjectId, ref: "seasons" }],
  },
  { timestamps: true }
);

mongoose.model("leagues", leagueSchema);
