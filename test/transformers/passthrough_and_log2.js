module.exports = function(doc) {
  console.log("transformer2: " + JSON.stringify(doc))
  return doc
}