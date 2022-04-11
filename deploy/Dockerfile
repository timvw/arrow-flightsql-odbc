# Copy the binary into a new container for a smaller docker image
FROM debian:buster-slim

RUN apt-get update
RUN apt-get install -y unixodbc
RUN apt-get install -y wget
RUN wget https://sfc-repo.snowflakecomputing.com/odbc/linux/latest/snowflake-odbc-2.24.7.x86_64.deb
RUN dpkg -i snowflake-odbc-*.x86_64.deb

ADD odbc.ini /etc/odbc.ini



