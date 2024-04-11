const Sequelize = require("sequelize");
const database = require("../services/database");

// Assuming you've already defined and imported League somewhere if needed for relationships

const MetaData = database.define(
  "meta_data",
  {
    season_id: {
      type: Sequelize.STRING,
      autoIncrement: false,
      allowNull: false,
      primaryKey: false,
      references: {
        model: "seasons", // This is the table name that Sequelize automatically generates
        key: "season_id",
      },
    },
    season_year: {
      type: Sequelize.INTEGER,
      autoIncrement: false,
      allowNull: false,
      primaryKey: false,
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
    current: {
      type: Sequelize.BOOLEAN,
      allowNull: true,
      default: false,
    },
    teams_updated: {
      type: Sequelize.BOOLEAN,
      allowNull: false,
      default: false,
    },
    matches_updated: {
      type: Sequelize.BOOLEAN,
      allowNull: false,
      default: false,
    },
    players_updated: {
      type: Sequelize.BOOLEAN,
      allowNull: false,
      default: false,
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

module.exports = MetaData;
