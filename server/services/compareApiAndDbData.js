function compareApiAndDbData(obj1, obj2) {
  return Object.keys(obj1).some((key) => obj1[key] !== obj2[key]);
}

module.exports = compareApiAndDbData;
