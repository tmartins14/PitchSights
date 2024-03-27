const Season = require("../../../models/Season");

const fetchAllSeasonIds = async () => {
  try {
    const seasons = await Season.findAll({ attributes: ["season_id"] });
    return seasons.map((season) => `sr:season:${season.dataValues.season_id}`);
  } catch (error) {
    console.log("Error fetching seasons from database: ", error);
  }
};

module.exports = fetchAllSeasonIds;
