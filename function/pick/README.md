# pick function

`pick()` will only include the specified fields from the message when sending down the pipeline. It currently only works for top level fields (i.e. `address.street` would not work).

### configuration

```javascript
pick({"fields": ["name"]})
```

### example

message in
```JSON
{
    "_id": 0,
    "name": "transporter",
    "type": "function"
}
```

config
```javascript
pick({"fields":["_id", "name"]})
```

message out
```JSON
{
    "_id": 0,
    "name": "transporter"
}
```