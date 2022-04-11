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



