#!/bin/sh

#if [ -z ${POSTGRES_PASSWORD+foo} ]; then
#    echo 'The environment variable POSTGRES_PASSWORD must be set' 1>&2
#    exit 1
#fi
#POSTGRES_PASSWORD='transitlog'

IMAGE_NAME='timescale/timescaledb-postgis:0.10.0-pg10'
HOST_DATA_DIR="${PWD}/data/"
CONTAINER_DATA_DIR='/var/lib/postgresql/data/'

DOCKER_HOST="${DOCKER_HOST:-localhost}"
CONTAINER_NAME="${CONTAINER_NAME:-timescaledb}"
BIN_CMD="${BIN_CMD:-postgres}"
SHARED_MEM_SIZE='1g'
PUBLISH_PORTS='54321:5432'

#TODO remember to set the password in prod scripts
#--env "POSTGRES_PASSWORD=${POSTGRES_PASSWORD}" \

#Note: if you want to recreate the container, we need to "release" the name
docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME

# TODO consider increasing maintenance_work_mem and max_wal_size.
# see https://www.postgresql.org/docs/10/populate.html


docker run \
    --detach \
    --restart unless-stopped \
    --name "${CONTAINER_NAME}" \
    --publish "${PUBLISH_PORTS}" \
    --volume "${HOST_DATA_DIR}:${CONTAINER_DATA_DIR}" \
    --shm-size="${SHARED_MEM_SIZE}" \
    --env "PGDATA=${CONTAINER_DATA_DIR}timescaledb" \
    "${IMAGE_NAME}" \
    "${BIN_CMD}" \
    -cshared_preload_libraries=timescaledb \
    -clog_line_prefix="%m [%p]: [%l-1] %u@%d" \
    -clog_error_verbosity=VERBOSE \
    ${1+"$@"}
