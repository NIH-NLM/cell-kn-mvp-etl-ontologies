#!/usr/bin/env bash
container_id=$(docker ps | grep arangodb | cut -d " " -f 1)
if [ -z "$container_id" ]; then
    docker run \
           -e ARANGO_ROOT_PASSWORD=$ARANGO_DB_PASSWORD \
           -p 8529:8529 \
           -d \
           -v $ARANGO_DB_HOME:/var/lib/arangodb3 \
           arangodb > /dev/null
fi
