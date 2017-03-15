# Elasticsearch adaptor

The [elasticsearch](https://www.elastic.co/) adaptor sends data to defined endpoints. List of
supported versions is below.

| Version | Note |
| --- | --- |
| 1.X | This version does not support bulk operations and will thus be much slower. |
| 2.X | Will only receive bug fixes, please consider upgrading. |
| 5.X | Most recent and supported version. |

***IMPORTANT***

If you want to keep the source `_id` as the elasticsearch document `_id`, transporter will
automatically do this. If you wish to use the auto-generated `_id` field for elasticsearch but would
like to retain the originating `_id` from the source, you'll need to include a transform function
similar to the following (assumes MongoDB source):

```javascript
module.exports = function(msg) {
   msg.data["mongo_id"] = msg.data._id['$oid']
   msg.data = _.omit(msg.data, ["_id"]);
   return msg;
}
```

***NOTE***
By using the elasticsearch auto-generated `_id`, it is not currently possible for transporter to
process update/delete operations. Future work is planned in [#39](https://github.com/compose/transporter/issues/39)
to address this problem.

### Configuration:
```javascript
es = elasticsearch({
  "uri": "https://username:password@hostname:port/thisgetsignored"
  "timeout": "10s" // optional, defaults to 30s
  "aws_access_key": "XXX" // optional, used for signing requests to AWS Elasticsearch service
  "aws_access_secret": "XXX" // optional, used for signing requests to AWS Elasticsearch service
})
```
