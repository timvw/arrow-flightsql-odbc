# Apache Arrow - FLight SQL - ODBC

Ideally databases expose an Apache Arrow Flight SQL service immediately.
Meanwhile, this project aims to implement an Apache Arrow Flight SQL server for ODBC data sources.

Links:
* https://arrow.apache.org/
* https://arrow.apache.org/docs/format/Flight.html
* https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/

Protocols:
* [Flight](https://github.com/apache/arrow/blob/master/format/Flight.proto)
* [FlightSql](https://github.com/apache/arrow/blob/master/format/FlightSql.proto)


## Development - Testing

Build container with snowflake and mariadb odbc drivers

```bash
docker build ./deploy -f ./deploy/Dockerfile -t odbc
```

Test connection, not using odbc.ini

```bash
export SNOW_URL=XXX
export SNOW_USER=XXX
export SNOW_PASS=XXX
export SNOW_DATABASE=XXX
export SNOW_WAREHOUSE=XXX
export SNOW_DRIVER=/usr/lib/snowflake/odbc/lib/libSnowflake.so
export SNOW_DSN="Driver=${SNOW_DRIVER};server=${SNOW_URL};UID=${SNOW_USER};PWD=${SNOW_PASS};database=${SNOW_DATABASE};warehouse=${SNOW_WAREHOUSE}"

docker run --rm -it odbc isql -k "${SNOW_DSN}"
```

docker build . -f ./deploy/Dockerfile -t odbc
docker run --rm -it --env-file ./.dockerenv odbc /bin/bash




