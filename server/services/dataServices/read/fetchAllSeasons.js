const Season = require("../../../models/Season");

const fetchAllSeasons = async () => {
  try {
    const seasons = await Season.findAll({
      attributes: ["season_year", "league_id"],
    });
    return seasons.map((season) => season.dataValues);
  } catch (error) {
    console.log("Error fetching seasons from database: ", error);
  }
};

module.exports = fetchAllSeasons;
