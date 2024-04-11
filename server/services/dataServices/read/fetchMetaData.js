const MetaData = require("../../../models/MetaData");
const { Op } = require("sequelize");

const fetchMetaData = async (field) => {
  let whereClause;
  if (field === "teams") {
    whereClause = { [Op.or]: [{ current: 1 }, { teams_updated: 0 }] };
  } else if (field === "matches") {
    whereClause = { [Op.or]: [{ current: 1 }, { matches_updated: 0 }] };
  } else if (field === "players") {
    whereClause = { [Op.or]: [{ current: 1 }, { players_updated: 0 }] };
  } else {
    whereClause = {};
  }

  try {
    const metaData = await MetaData.findAll({
      attributes: ["season_id", "season_year", "league_id"],
      where: whereClause,
    });
    return metaData.map((data) => data.dataValues);
  } catch (error) {
    console.log("Error fetching meta data from database: ", error);
  }
};

module.exports = fetchMetaData;
