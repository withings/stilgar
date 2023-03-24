FROM bitnami/clickhouse

# Image fix: entry point waits for a log that never happens when using docker-entrypoint-startdb.d
RUN sed 's/.*Application: Ready for connections.*/    sleep 2/' -i /opt/bitnami/scripts/libclickhouse.sh

COPY docker-entrypoint-startdb.d/db-init.sh /docker-entrypoint-startdb.d
