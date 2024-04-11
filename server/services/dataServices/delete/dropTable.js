const { database, models } = require("../../../models/models"); // Adjust the path as necessary
const MetaData = require("../../../models/MetaData");

// Function to drop a table based on the model name
async function dropTable(modelName) {
  try {
    await database.authenticate(); // Ensure we can connect to the database
    console.log(
      "Connection to the database has been established successfully."
    );

    console.log(database.models);

    // Lookup the model by name
    const model = database.models[modelName];
    if (!model) {
      console.log(`Model "${modelName}" does not exist.`);
      return;
    }

    // Drop the table associated with the model
    await model.drop();

    if (modelName === "teams") {
      await MetaData.update({ teams_updated: false }, { where: {} });
    } else if (modelName === "matches") {
      await MetaData.update({ matches_updated: false }, { where: {} });
    } else if (modelName === "players") {
      await MetaData.update({ players_updated: false }, { where: {} });
    }
    console.log(
      `Table for model "${modelName}" has been successfully deleted.`
    );
  } catch (error) {
    console.error(`Error deleting table for model "${modelName}":`, error);
  }
}

// Example usage: Replace 'leagues' with the name of the model you wish to drop
dropTable("matches");
