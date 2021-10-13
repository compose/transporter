# File to boot haproxy 2.0 and use rabbitmq running on your local machine on port 5672

FROM haproxy:2.0

RUN mkdir -p /tmp/rabbitmq
COPY certs/rabbitmq.pem certs/ca.pem haproxy.cfg /tmp/rabbitmq/
WORKDIR /tmp/rabbitmq
CMD ["haproxy", "-V", "--", "haproxy.cfg"]
