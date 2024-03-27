const Sequelize = require("sequelize");
const database = require("../services/database");

// Assuming you've already defined and imported League somewhere if needed for relationships

const Season = database.define(
  "seasons",
  {
    season_id: {
      type: Sequelize.INTEGER,
      autoIncrement: true,
      allowNull: false,
      primaryKey: true,
    },
    league_id: {
      type: Sequelize.INTEGER,
      allowNull: false,
      references: {
        model: "leagues", // This is the table name that Sequelize automatically generates
        key: "league_id",
      },
      default: -1,
    },
    season_name: {
      type: Sequelize.STRING,
      allowNull: false,
    },
    start_date: {
      type: Sequelize.DATEONLY,
      allowNull: false,
    },
    end_date: {
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
