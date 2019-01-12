#!/bin/sh

IMAGE_NAME='cassandra:3.11.3'
HOST_DATA_DIR="${PWD}/cassandra-data/"
CONTAINER_DATA_DIR='/var/lib/cassandra/'

DOCKER_HOST="${DOCKER_HOST:-localhost}"
CONTAINER_NAME="${CONTAINER_NAME:-cassandra}"
BIN_CMD="${BIN_CMD:-cassandra}"
PUBLISH_PORTS='9042:9042'

#Note: if you want to recreate the container, we need to "release" the name
docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME

docker run --name "${CONTAINER_NAME}" -d --publish "${PUBLISH_PORTS}" "${IMAGE_NAME}"

#docker run \
#    --detach \
#    --restart unless-stopped \
#    --name "${CONTAINER_NAME}" \
#    --publish "${PUBLISH_PORTS}" \
#    --volume "${HOST_DATA_DIR}:${CONTAINER_DATA_DIR}" \
#    "${IMAGE_NAME}" \
#    "${BIN_CMD}"