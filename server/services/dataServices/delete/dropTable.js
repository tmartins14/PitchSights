const { database, models } = require("../../../models/index"); // Adjust the path as necessary

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
    console.log(
      `Table for model "${modelName}" has been successfully deleted.`
    );
  } catch (error) {
    console.error(`Error deleting table for model "${modelName}":`, error);
  }
}

// Example usage: Replace 'leagues' with the name of the model you wish to drop
dropTable("leagues");
