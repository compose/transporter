# Mongo configuration

## How to generate a new self-signed TLS cert for mongo

```sh
openssl req -newkey rsa:2048 -new -x509 -days 3650 -nodes -subj '/C=US/ST=Massachusetts/L=Bedford/O=Personal/OU=Personal/emailAddress=example@example.com/CN=transporter-mongo' -out mongodb-cert.crt -keyout mongodb-cert.key
cat mongodb-cert.key mongodb-cert.crt > mongodb.pem
cp mongodb-cert.crt mongodb-ca.crt
```

TRY THIS, redo
```sh
openssl req -nodes -out ca.pem -keyout server.key -new -x509 -days 3650 -subj "/C=AU/ST=NSW/O=Organisation/CN=root/emailAddress=user@domain.com" -addext "subjectAltName = DNS:transporter-mongo"
echo "00" > file.srl
# openssl genrsa -out server.key 2048
openssl req -key server.key -new -out server.req -subj "/C=AU/ST=NSW/O=Organisation/CN=server1/CN=transporter-mongo/emailAddress=user@domain.com"
openssl x509 -req -in server.req -CA ca.pem -CAkey server.key -CAserial file.srl -out server.crt -days 3650
cat server.key server.crt > server.pem
openssl verify -CAfile ca.pem server.pem


openssl genrsa -out client.key 2048
openssl req -key client.key -new -out client.req -subj "/C=AU/ST=NSW/O=Organisation/CN=client1/emailAddress=user@domain.com" -addext "subjectAltName = DNS:transporter-mongo"
openssl x509 -req -in client.req -CA ca.pem -CAkey server.key -CAserial file.srl -out client.crt -days 3650
cat client.key client.crt > client.pem
openssl verify -CAfile ca.pem client.pem
```
