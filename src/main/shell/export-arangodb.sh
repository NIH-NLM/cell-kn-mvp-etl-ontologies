#!/usr/bin/env bash
container_id=$(docker ps | grep arangodb | cut -d " " -f 1)
if [ -n "$container_id" ]; then
    docker exec $container_id arangoexport \
        --server.endpoint tcp://127.0.0.1:8529 \
        --server.username root \
        --server.password $ARANGO_DB_PASSWORD \
        --server.database Cell-KN-Ontologies \
        --collection CL \
        --output-directory "exports"
    docker cp $container_id:/exports .
fi
