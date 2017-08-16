# opfilter function

`opfilter()` will skip messages based on the provided Whitelist or Blacklist ops. The string representation
of each operation can be found in the [message/ops pkg](https://github.com/compose/transporter/blob/master/message/ops/ops.go#L22-L41).

### configuration

```javascript
opfilter({"whitelist": ["insert"]})
```

```javascript
opfilter({"blacklist": ["delete"]})
```

### example

messages in
```JSON
{
    "op": "insert",
    {
        "_id": 0,
        "name": "transporter",
        "type": "function",
        "count": 10
    }
}
{
    "op": "delete",
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
opfilter({"whitelist": ["insert"]})
```

messages out
```JSON
{
    "op": "insert",
    {
        "_id": 0,
        "name": "transporter",
        "type": "function",
        "count": 10
    }
}
```