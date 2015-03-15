module.exports = function(doc) {
  doc["_id"] = doc["id"];
  delete doc["id"];

  return doc;
}
