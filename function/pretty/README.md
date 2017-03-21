# pretty function

`pretty()` will marshal the data to JSON and then log it at the `INFO` level. The default indention setting is `2` spaces and if set to `0`, it will print on a single line.

### configuration

```javascript
pretty({"spaces": 2})
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
pretty({"spaces":0})
```

log line
```shell
INFO[0000]
{"_id":0,"name":"transporter","type":"function"}
```

config
```javascript
pretty({"spaces":2})
```

log line
```shell
INFO[0000]
{
  "_id":0,
  "name":"transporter",
  "type":"function"
}
```