// const mongoose = require("mongoose");
// const { Schema } = mongoose;

// const leagueSchema = new Schema(
//   {
//     leagueId: { type: String, required: true },
//     leagueName: String,
//     country: String,
//     gender: { type: String, required: true },
//   },
//   { timestamps: true }
// );

// const League = mongoose.model("leagues", leagueSchema);

// module.exports = League;

const Sequelize = require("sequelize");
const sequelize = require("../services/database"); // Adjust the path as necessary

const League = sequelize.define(
  "league",
  {
    leagueId: {
      type: Sequelize.INTEGER,
      allowNull: false,
      primaryKey: true,
    },
    leagueName: {
      type: Sequelize.STRING,
      allowNull: false,
    },
    country: {
      type: Sequelize.STRING,
      allowNull: false,
    },
    gender: {
      type: Sequelize.STRING,
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

module.exports = League;
