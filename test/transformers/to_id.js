module.exports = function(doc) {
  doc.data._id = doc.data["id"]
  delete doc.data["id"]
  return doc
}
