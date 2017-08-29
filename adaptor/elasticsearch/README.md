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

***NOTES***

By using the elasticsearch auto-generated `_id`, it is not currently possible for transporter to
process update/delete operations. Future work is planned in [#39](https://github.com/compose/transporter/issues/39)
to address this problem.

If no `INDEX_NAME` is provided, transporter will configure a default index named `test`.

### Configuration:
```javascript
es = elasticsearch({
  "uri": "https://username:password@hostname:port/INDEX_NAME"
  "timeout": "10s" // optional, defaults to 30s
  "aws_access_key": "XXX" // optional, used for signing requests to AWS Elasticsearch service
  "aws_access_secret": "XXX" // optional, used for signing requests to AWS Elasticsearch service
  "parent_id": "elastic_parent" // optional, used for specifying parent-child relationships
})
```

***NOTES***

When writing to Elasticsearch with larger documents or any complex inserts that requires longer Elasticsearch timeouts, increase your write timeouts with `Config({"write_timeout":"30s"})` in addition to adapter level configuration to prevent concurrency issues.

```javascript
t.Config({"write_timeout":"30s"}).Source("source", source).Save("sink", sink)
```

Addressing [#391](https://github.com/compose/transporter/issues/391)

### Parent-Child Relationships

*Note*
Only Elasticsearch 5.x is being supported at the moment.

If you have parent-child relationships in your data, specify `parent_id` in the configs.

Be sure to add your [parent-child mapping](https://www.elastic.co/guide/en/elasticsearch/guide/current/parent-child-mapping.html) and make sure that your elasticsearch `_id` in your parent corresponds with the `parent_id` that you specified in your configs.

Check that after you add your parent-child mapping, that data is getting inserted properly.

### Example of Parent-Child Mapping:

This step is manual, you must set your mapping manually using a `PUT` request to Elasticsearch.

PUT `/<your index name>`

```
{
   "mappings":{
      "company":{},
      "employee":{
         "_parent":{
            "type":"company"
         }
      }
   }
}
```

#### Then your transporter configs will be something like:

```
es = elasticsearch({
  "uri": "https://username:password@hostname:port/INDEX_NAME"
  "timeout": "10s"
  "aws_access_key": "XXX"
  "aws_access_secret": "XXX"
  "parent_id": "company_id"
})
```

#### Sample Data for the config above to insert:

In this sample dataset, a company has many employees.

Note: `company_id` is the parent reference, below .

##### Company

```
{"_id": "9g2g", "name": "gingerbreadhouse"}
```

##### Employee

```
{"_id": "9g2g", "name": "hansel", "company_id": "gingerbreadhouse"}
{"_id": "9g4g", "name": "gretel", "company_id": "gingerbreadhouse"}
{"_id": "9g6g", "name": "witch", "company_id": "gingerbreadhouse"}
```

Caution: If you try to insert / update data without the mapping step, the inserts will fail. Run transporter with the debug flag to see errors.

`transporter run -log.level=debug`


