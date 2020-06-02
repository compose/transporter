# remap function

`remap()` will replace existing namespaces with new ones based on the provided configuration.

### configuration

```javascript
remap({"ns_map": {"foo":"bar"}})
```

### example

message in
```JSON
{
    "namespace": "foo",
    {
        "_id": 0,
        "name": "transporter",
        "type": "function",
        "count": 10
    }
}
```

config
```javascript
remap({"ns_map": {"foo":"bar"}})
```

message out
```JSON
{
    "namespace": "bar",
    {
        "_id": 0,
        "name": "transporter",
        "type": "function",
        "count": 10
    }
}
```