# Elasticsearch adaptor

The [elasticsearch](https://www.elastic.co/) adaptor sends data to defined endpoints.

***IMPORTANT***

It is currently not possible to overwrite the auto-generated `_id` field for elasticsearch but if you would like to retain
the originating `_id` from the source, you'll need to include a transform function as follows (assumes MongoDB source):

```javascript
module.exports = function(msg) {
   msg.data["mongo_id"] = msg.data._id['$oid']
   return msg;
}
```

### Configuration:
```yaml
- es:
  type: elasticsearch
  uri: https://username:password@hostname:port/thisgetsignored
```
