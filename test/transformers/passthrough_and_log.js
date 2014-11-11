module.exports = function(doc) {
  console.log("transformer: " + JSON.stringify(doc))
  return doc
}