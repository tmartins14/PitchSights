const { database, models } = require("../../../models/index"); // Adjust the path as necessary

// Function to create a table based on the model name
async function createTable(modelName) {
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

    // Create the table associated with the model
    await model.sync(); // You can use { force: true } to drop the table if it already exists and recreate it
    console.log(
      `Table for model "${modelName}" has been successfully created.`
    );
  } catch (error) {
    console.error(`Error creating table for model "${modelName}":`, error);
  }
}

// Example usage: Replace 'leagues' with the name of the model you wish to drop
createTable("seasons");
