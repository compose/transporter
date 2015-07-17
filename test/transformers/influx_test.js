module.exports = function(doc) { doc["data"] = _.pick(doc.data, ["_id", "download_count"]); return doc }
