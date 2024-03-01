const mongoose = require("mongoose");
const { Schema } = mongoose;

const seasonSchema = new Schema(
  {
    seasonId: { type: String, required: true },
    seasonName: { type: String, required: true },
    league: { type: Schema.Types.ObjectId, ref: "leagues" },
    year: String,
    startDate: Date,
    endDate: Date,
  },
  { timestamps: true }
);

mongoose.model("seasons", seasonSchema);
