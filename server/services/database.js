const Sequelize = require("sequelize");

const database = new Sequelize("pitchsightsDB", "root", "Lambeau1992", {
  dialect: "mysql",
  host: "localhost",
});

module.exports = database;
