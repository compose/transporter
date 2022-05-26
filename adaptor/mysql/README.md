# MySQL adaptor

## Using the adaptor

You need to specify a sink and source like so:

```
var source = mysql({
  "uri": "mysql://user:pass@source.host.com:11111/database?ssl=custom",
  "tail": true,
  "cacert": "/path/to/source.crt",
})

var sink = mysql({
  "uri": "mysql://user:pass@sink.host.com:22222/database?ssl=custom",
  "cacert": "/path/to/sink.crt",
  "servername": "sink.host.com",
})

t.Source("source", source, "/.*/").Save("sink", sink, "/.*/")
```

- tailing is optional and only makes sense on the source
- For TLS you can use `ssl=true` which does unverified TLS or `ssl=custom` in
which case you need to supply the `cacert`.
- You don't need to supply the `servername`, but if you do the certificate will
be verified against it

### Requirements

- The source must allow the connecting user to query the binlog
- Per Postgresql you need to create the sink/destination table structure first

### Limitations

- Note that per the Postgresql adaptor this probably isn't very performant at
copying huge databases as there is no bulk option yet.
- Has only been developed and tested using MySQL as the sink and source. Unsure
how it will function when combined with other adaptors.
