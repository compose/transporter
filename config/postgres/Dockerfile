FROM ubuntu:latest
ARG VERSION
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && \
    apt install -y wget netcat lsb-release gnupg2


RUN echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt install -y postgresql-$VERSION

COPY scripts/ /tmp/postgresql/scripts/
COPY ph_hba.conf /etc/postgresql/$VERSION/main/pg_hba.conf

CMD ["/tmp/postgresql/scripts/setup_and_run_postgresql"]
