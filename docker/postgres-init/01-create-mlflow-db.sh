#!/bin/sh
# Create the database MLflow expects (spec §15).
#
# /bin/sh, not /bin/bash: postgres:17-alpine ships busybox ash and no bash at all, so a
# bash shebang fails with "bad interpreter" *after* the database has already initialised
# — leaving a volume that looks healthy and has no mlflow database in it.
#
# The v3 stack pointed MLflow at a database the Postgres service never created, so the
# science profile died on startup with a connection error that looked like a networking
# problem. It was not: the database simply did not exist.
#
# The official image runs everything in /docker-entrypoint-initdb.d exactly once, on the
# first initialisation of the data volume. Re-running `docker compose up` does not
# re-run this; `make down`, which removes volumes, does.
set -euo pipefail

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE mlflow OWNER $POSTGRES_USER;
EOSQL

echo "created database: mlflow"
