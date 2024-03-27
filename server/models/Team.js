const Sequelize = require("sequelize");
const sequelize = require("../services/database");

const Team = sequelize.define(
  "teams",
  {
    competitor_id: {
      type: Sequelize.INTEGER,
      allowNull: false,
      primaryKey: true,
    },
    full_name: {
      type: Sequelize.STRING,
      allowNull: false,
    },
    short_name: {
      type: Sequelize.STRING,
      allowNull: false,
    },
    abbv: {
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

module.exports = Team;
