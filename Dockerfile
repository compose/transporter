FROM golang:1.9 as builder

# Setting up working directory
ADD . /go/src/github.com/compose/transporter/
WORKDIR /go/src/github.com/compose/transporter/

ARG VERSION

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o transporter -ldflags="-X main.version=$VERSION" ./cmd/transporter/...

FROM alpine:latest 

RUN apk --no-cache add ca-certificates

COPY --from=builder /go/src/github.com/compose/transporter/transporter /usr/local/bin/

# Alpine Linux doesn't use pam, which means that there is no /etc/nsswitch.conf,
# but Golang relies on /etc/nsswitch.conf to check the order of DNS resolving
# (see https://github.com/golang/go/commit/9dee7771f561cf6aee081c0af6658cc81fac3918)
# To fix this we just create /etc/nsswitch.conf and add the following line:
RUN echo 'hosts: files mdns4_minimal [NOTFOUND=return] dns mdns4' >> /etc/nsswitch.conf

CMD ["/usr/local/bin/transporter"]