module.exports = function(doc) {
  doc.ns = doc.ns.replace("boom.", "boomdest.public.") // remove the database name

  doc.data.id = doc.data["_id"]["$oid"] // get the string value for the oid
  delete doc.data["_id"]
  return doc
}
