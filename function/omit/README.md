# omit function

`omit()` will remove any fields specified from the message and then send down the pipeline. It currently only works for top level fields (i.e. `address.street` would not work).

### configuration

```javascript
omit({"fields": ["name"]})
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
omit({"fields":["type"]})
```

message out
```JSON
{
    "_id": 0,
    "name": "transporter"
}
```