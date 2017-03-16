# skip function

`skip()` will evalute the data based on the criteria configured and determine whether the message should continue down the pipeline or be skipped. When evaluating the data, `true` will result in the message being sent down the pipeline and `false` will result in the message being skipped. Take a look at the [tests](skipper_test.go) for all currently supported configurations. It currently only works for top level fields (i.e. `address.street` would not work).

### configuration

```javascript
skip({"field": "test", "operator": "==", "match": 10})
```

### example

message in
```JSON
{
    "_id": 0,
    "name": "transporter",
    "type": "function",
    "count": 10
}
```

config
```javascript
skip({"field": "count", "operator": "==", "match": 10})
```

message out
```JSON
{
    "_id": 0,
    "name": "transporter",
    "type": "function",
    "count": 10
}
```

config
```javascript
skip({"field": "count", "operator": ">", "match": 20})
```

message would be skipped