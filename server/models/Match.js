const Sequelize = require("sequelize");
const database = require("../services/database");

// Assuming you've already defined and imported League somewhere if needed for relationships

const Match = database.define(
  "matches",
  {
    sport_event_id: {
      type: Sequelize.INTEGER,
      autoIncrement: false,
      allowNull: false,
      primaryKey: true,
    },
    competition_id: {
      type: Sequelize.INTEGER,
      allowNull: false,
      references: {
        model: "leagues", // This is the table name that Sequelize automatically generates
        key: "competition_id",
      },
      default: -1,
    },
    season_id: {
      type: Sequelize.INTEGER,
      allowNull: false,
      references: {
        model: "seasons", // This is the table name that Sequelize automatically generates
        key: "season_id",
      },
    },

    home_team_id: {
      type: Sequelize.INTEGER,
      allowNull: false,
      references: {
        model: "teams",
        key: "competitor_id",
      },
      default: -1,
    },
    away_team_id: {
      type: Sequelize.INTEGER,
      allowNull: false,
      references: {
        model: "teams", // This is the table name that Sequelize automatically generates
        key: "competitor_id",
      },
      default: -1,
    },
    winner_id: {
      type: Sequelize.INTEGER,
      allowNull: true,
      references: {
        model: "teams",
        key: "competitor_id",
      },
      default: -1,
    },
    home_score: {
      type: Sequelize.INTEGER,
      allowNull: true,
    },
    away_score: {
      type: Sequelize.INTEGER,
      allowNull: true,
    },
    start_time: {
      type: Sequelize.DATEONLY,
      allowNull: false,
    },
    venue: {
      type: Sequelize.STRING,
      allowNull: true,
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

module.exports = Match;
