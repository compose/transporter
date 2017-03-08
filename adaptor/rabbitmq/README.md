# RabbitMQ adaptor

The [RabbitMQ](http://www.rabbitmq.com/) adaptor is capable of consuming and publishing JSON data.

When being used to publish data, you need to configure the `routing_key` and the exchange is pulled
from the message `namespace` (i.e. database collection/table). If `key_in_field` is set to true,
transporter will use the field defined `routing_key` to lookup the value from the data.

***NOTE***
`key_in_field` defaults to false and will therefore use the static `routing_key`, if you
set `routing_key` to an empty string, no routing key will be set in the published message.

### Configuration:
```yaml
- rabbitmq:
    type: rabbitmq
    uri: amqp://127.0.0.1:5672/
    routing_key: "test"
    key_in_field: false
    # delivery_mode: 1 # non-persistent (1) or persistent (2)
    # api_port: 15672
    # ssl: false
    # cacerts: ["/path/to/cert.pem"]
```
