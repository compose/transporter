# rename function

`rename()` will update the replace existing key names with new ones based on the provided configuration. It currently only works for top level fields (i.e. `address.street` would not work).

### configuration

```javascript
rename({"field_map": {"test":"renamed"}})
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
rename({"field_map": {"count":"total"}})
```

message out
```JSON
{
    "_id": 0,
    "name": "transporter",
    "type": "function",
    "total": 10
}
```