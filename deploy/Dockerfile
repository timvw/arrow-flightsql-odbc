FROM rust:1.60 as builder
RUN apt-get update
RUN apt-get install -y cmake musl-tools clang libc++-dev build-essential autoconf libtool pkg-config
RUN apt-get install -y unixodbc-dev

FROM builder AS build
WORKDIR /opt/app

# create an empty project
RUN mkdir -p ./src/bin && \
    echo "fn main() {}" > ./src/bin/test.rs

# copy over your manifests
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# this build step will cache your dependencies
RUN cargo build --release

# copy your source tree
COPY ./build.rs ./build.rs
COPY ./proto ./proto
COPY ./src ./src

# build for release
RUN cargo build --release

# Copy the binary into a new container for a smaller docker image
FROM debian:buster-slim

RUN apt-get update
RUN apt-get install -y unixodbc wget
RUN wget https://sfc-repo.snowflakecomputing.com/odbc/linux/latest/snowflake-odbc-2.24.7.x86_64.deb
RUN dpkg -i snowflake-odbc-*.x86_64.deb
#RUN wget https://dlm.mariadb.com/1936451/Connectors/odbc/connector-odbc-3.1.15/mariadb-connector-odbc-3.1.15-debian-buster-amd64.tar.gz
#RUN tar -zxf mariadb-connector-odbc-3.1.15-debian-buster-amd64.tar.gz
#RUN chown -R root:root /mariadb-connector-odbc-3.1.15-debian-buster-amd64
RUN wget https://downloads.mariadb.com/Connectors/odbc/connector-odbc-2.0.15/mariadb-connector-odbc-2.0.15-ga-debian-x86_64.tar.gz
RUN tar -xzf mariadb-connector-odbc-2.0.15-ga-debian-x86_64.tar.gz

WORKDIR /opt/app
COPY --from=build /opt/app/target/release/flightsql-odbc-server /opt/app/flightsql-odbc-server

EXPOSE 52358
ENV SERVER_ADDRESS="0.0.0.0:52358"
ENTRYPOINT [ "/opt/app/flightsql-odbc-server" ]



