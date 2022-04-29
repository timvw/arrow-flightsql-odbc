# Apache Arrow - FLight SQL - ODBC

Ideally databases expose an Apache Arrow Flight SQL service immediately.
Meanwhile, this project aims to implement an Apache Arrow Flight SQL server for ODBC data sources.

Currently the following commands are implemented (but far from optimal):
* CommandStatementQuery 
* CommandGetTables

## Running the server on your machine

The server tries to connect to a SQL server via an ODBC connector. 
This needs to be specified via the ODBC_CONNECTION_STRING environment variable.

On my mac I have installed the mariadb-connector-odbc driver (brew install mariadb-connector-odbc)
and I have an instance running on localhost.

```bash
export ODBC_CONNECTION_STRING="Driver=/usr/local/Cellar/mariadb-connector-odbc/3.1.15/lib/mariadb/libmaodbc.dylib;SERVER=localhost;USER=demo;PASSWORD=demo;PORT=3306;database=demo"
cargo run server
```


## Running the server in a container

As on your local machine, the essential piece is to have a working ODBC_CONNECTION_STRING (driver + connectiond details).
Here is how you can build a container with [snowflake](https://docs.snowflake.com/en/user-guide/odbc.html) and [mysql](https://dev.mysql.com/downloads/connector/odbc/) odbc drivers.

```bash
docker build . -f ./deploy/Dockerfile -t flightsql-odbc-server
```

The paths to those drivers are:
* /usr/lib/snowflake/odbc/lib/libSnowflake.so
* /mariadb-connector-odbc-2.0.15-ga-debian-x86_64/lib/libmaodbc.so

```bash
export SNOW_ODBC_CONNECTION_STRING="Driver=/usr/lib/snowflake/odbc/lib/libSnowflake.so;Server=account.eu-central-1.snowflakecomputing.com;UID=DEMO;PWD=DEMO;database=TEST_DEMO_DB;warehouse=DEMO_DWH"
export MYSQL_ODBC_CONNECTION_STRING="Driver=/mariadb-connector-odbc-2.0.15-ga-debian-x86_64/lib/libmaodbc.so;SERVER=hostname;USER=demo;PASSWORD=demo;PORT=3306;database=demo"
docker run --rm -it -e ODBC_CONNECTION_STRING="$MYSQL_ODBC_CONNECTION_STRING" -p 52358:52358 flightsql-odbc-server
```

## Resources

Links:
* https://arrow.apache.org/
* https://arrow.apache.org/docs/format/Flight.html
* https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/

Protocols:
* [Flight](https://github.com/apache/arrow/blob/master/format/Flight.proto)
* [FlightSql](https://github.com/apache/arrow/blob/master/format/FlightSql.proto)

Some code is copied from https://github.com/apache/arrow-rs/tree/master/arrow-flight/src/sql





