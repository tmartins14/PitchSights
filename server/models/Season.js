// const mongoose = require("mongoose");
// const { Schema } = mongoose;

// const seasonSchema = new Schema(
//   {
//     seasonId: { type: String, required: true },
//     seasonName: { type: String, required: true },
//     league: { type: Schema.Types.ObjectId, ref: "leagues" },
//     year: String,
//     startDate: Date,
//     endDate: Date,
//   },
//   { timestamps: true }
// );

// const Season = mongoose.model("seasons", seasonSchema);

// module.exports = Season;

const Sequelize = require("sequelize");
const database = require("../services/database");

// Assuming you've already defined and imported League somewhere if needed for relationships

const Season = database.define(
  "seasons",
  {
    seasonId: {
      type: Sequelize.INTEGER,
      autoIncrement: true,
      allowNull: false,
      primaryKey: true,
    },
    leagueId: {
      type: Sequelize.INTEGER,
      allowNull: false,
      references: {
        model: "leagues", // This is the table name that Sequelize automatically generates
        key: "leagueId",
      },
    },
    name: {
      type: Sequelize.STRING,
      allowNull: false,
    },
    startDate: {
      type: Sequelize.DATEONLY,
      allowNull: false,
    },
    endDate: {
      type: Sequelize.DATEONLY,
      allowNull: false,
    },
    updatedAt: {
      type: Sequelize.DATE,
      allowNull: false,
    },
  },
  {
    // Disable timestamps or specifically the updatedAt field
    timestamps: true, // Keep it true if you want the createdAt field to be automatically managed
    createdAt: false, // Specifically disable the updatedAt functionality
  }
);

module.exports = Season;
