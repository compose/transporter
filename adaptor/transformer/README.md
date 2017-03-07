# transformer adaptor

The transformer adaptor receives and sends data through the defined javascript function for processing.

The parameter passed to the function has been converted from a go `map[string]interface{}` to a JS object of the following form:

```javascript
{
    "ns":"message.namespace",
    "ts":12345, // time represented in milliseconds since epoch
    "op":"insert",
    "data": {
        "id": "abcdef",
        "name": "hello world"
    }
}
```

***NOTE*** when working with data from MongoDB, the `_id` field will be represented in the following fashion:

```javascript
{
    "ns":"message.namespace",
    "ts":12345, // time represented in milliseconds since epoch
    "op":"insert",
    "data": {
        "_id": {
            "$oid": "54a4420502a14b9641000001"
        },
        "name": "hello world"
    }
}
```

There are two types of JavaScript VMs available, `otto` and `goja`. You can configure which one to use via the YAML configration and each has its own JavaScript function signature. The `goja` VM has shown better performance in benchmarks but it does *NOT* include the underscore library.

The default JavaScript VM is `otto` as we are trying to maintain backwards compatability for users but that may change in the future.

### Configuration
```yaml
- logtransformer:
    filename: transform.js
    type: transformer
    # vm: otto
```

### otto VM
```javascript
module.exports=function(doc) {
    console.log(doc['ns']);
    console.log(doc['ts']);
    console.log(doc['op']);
    console.log(doc['data']);
    return doc
}
```

### goja VM
```javascript
function transform(doc) {
    return doc
}
```
