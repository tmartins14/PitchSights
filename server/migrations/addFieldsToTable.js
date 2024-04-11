"use strict";

/** @type {import('sequelize-cli').Migration} */
module.exports = {
  async up(queryInterface, Sequelize) {
    await queryInterface.addColumn("meta_data", "players_updated", {
      type: Sequelize.BOOLEAN,
      allowNull: false,
      default: false, // or false, depending on your needs
    });
  },

  async down(queryInterface, Sequelize) {
    // await queryInterface.removeColumn("YourTableName", "newColumn");
  },
};
