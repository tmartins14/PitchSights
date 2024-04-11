const Sequelize = require("sequelize");
const keys = require("../config/keys");

const database = new Sequelize(keys.mySQLDB, "root", keys.mySQLPassword, {
  dialect: "mysql",
  host: "localhost",
});

module.exports = database;
