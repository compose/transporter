// This config file works for ssl connection; One time migration
var source = mongodb({
  "uri": "mongodb://root:Cl0udte_m@dds-t4n6e9e06019c9841101-pub.mongodb.singapore.rds.aliyuncs.com:3717,dds-t4n6e9e06019c9842112-pub.mongodb.singapore.rds.aliyuncs.com:3717/ali1?replicaSet=mgset-303314532&authSource=admin"
  "timeout": "30s",
  "tail": false,
  //"ssl": true,
  "cacerts": ["/Users/cyndi/go/src/github.com/compose/transporter_original/ApsaraDB-CA-Chain.pem"]
  "ssl_allow_invalid_hostnames": true,
  // "wc": 1,
  // "fsync": false,
  // "bulk": false,
  // "collection_filters": "{}",
  // "read_preference": "Primary"
})
var sink = mongodb({
  "uri": "mongodb://admin:LetmelearnsthfromMongo@45a53dfa-e491-48c5-b947-df498857bb23-0.bkvfu0nd0m8k95k94ujg.databases.appdomain.cloud:30911,45a53dfa-e491-48c5-b947-df498857bb23-1.bkvfu0nd0m8k95k94ujg.databases.appdomain.cloud:30911/ibm1?authSource=admin&replicaSet=replset"
  "timeout": "30s",
  "tail": false,
  "ssl": true,
  "cacerts": ["/Users/cyndi/fa1498a3-0bba-11ea-9a2f-deb1275e52d0"],
  // "wc": 1,
  // "fsync": false,
  // "bulk": false,
  // "collection_filters": "{}",
  // "read_preference": "Primary"
})
t.Source("source", source, "/.*/").Save("sink", sink, "/.*/")
