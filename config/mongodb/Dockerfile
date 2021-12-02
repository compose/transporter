FROM ubuntu:bionic
ARG VERSION

RUN apt update && \
    apt install -y python-pip wget curl libssl1.0.0 && \
    pip install "mongo-orchestration>=0.6.7,<1.0"

RUN wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu1404-$VERSION.tgz && \
    mkdir -p /tmp/mongodb-linux-x86_64-ubuntu1404-$VERSION && \
    tar xfz mongodb-linux-x86_64-ubuntu1404-$VERSION.tgz -C /tmp && \
    rm mongodb-linux-x86_64-ubuntu1404-$VERSION.tgz && \
    mkdir -p /tmp/mongodb/scripts /tmp/mongodb/configurations

COPY certs/* /tmp/mongodb/
COPY configurations/ /tmp/mongodb/configurations/
COPY scripts/ /tmp/mongodb/scripts/

RUN echo "export PATH=/tmp/mongodb-linux-x86_64-ubuntu1404-$VERSION/bin:$PATH" > /tmp/mongodb/env

CMD ["/tmp/mongodb/scripts/setup_and_run_mongo"]
