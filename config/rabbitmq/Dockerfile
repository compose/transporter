# Image used to run an haproxy in front of rabbitmq to provide TLS support.

FROM haproxy:2.0

RUN mkdir -p /tmp/rabbitmq
COPY certs/rabbitmq.pem certs/ca.pem haproxy.cfg /tmp/rabbitmq/
WORKDIR /tmp/rabbitmq
CMD ["haproxy", "-V", "--", "haproxy.cfg"]
