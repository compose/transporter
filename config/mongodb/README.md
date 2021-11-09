# Mongo configuration

## How to generate a new self-signed TLS cert for mongo tests

```sh
# Server
openssl req -nodes -out ca.pem -keyout server.key -new -x509 -days 3650 -subj "/C=AU/ST=NSW/O=Organisation/CN=root/emailAddress=user@domain.com" -addext "subjectAltName = DNS:transporter-db"
echo "00" > file.srl
openssl req -key server.key -new -out server.req -subj "/C=AU/ST=NSW/O=Organisation/CN=server1/CN=transporter-db/emailAddress=user@domain.com"
openssl x509 -req -in server.req -CA ca.pem -CAkey server.key -CAserial file.srl -out server.crt -days 3650
cat server.key server.crt > server.pem
openssl verify -CAfile ca.pem server.pem

# Client
openssl genrsa -out client.key 2048
openssl req -key client.key -new -out client.req -subj "/C=AU/ST=NSW/O=Organisation/CN=client1/emailAddress=user@domain.com" -addext "subjectAltName = DNS:transporter-db"
openssl x509 -req -in client.req -CA ca.pem -CAkey server.key -CAserial file.srl -out client.crt -days 3650
cat client.key client.crt > client.pem
openssl verify -CAfile ca.pem client.pem
```

Then, rebuild the mongo image used for testing
Then copy `ca.pem`, `client.key` and `client.crt` to adaptor/mongodb/testdata
