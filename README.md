# ARROW FLight SQL ODBC

Build container with snowflake odbc driver

```bash
docker build ./deploy -f ./deploy/Dockerfile -t odbc
```

Test connection to snowflake (assumes you have SNOW_USER and SNOW_PASS envioronment variables)
```bash
docker run --rm -it odbc isql -v testodbc1 $SNOW_USER $SNOW_PASS
```

Test connection, not using odbc.ini

```bash
docker run --rm -it odbc isql -k "Driver=/usr/lib/snowflake/odbc/lib/libSnowflake.so;server=dt28750.eu-central-1.snowflakecomputing.com;UID=$SNOW_USER;PWD=$SNOW_PASS"
```

