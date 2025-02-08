#!/usr/bin/env bash
container_id=$(docker ps | grep arangodb | cut -d " " -f 1)
if [ -n "$container_id" ]; then
   docker container stop $container_id > /dev/null
   docker container rm $container_id > /dev/null
fi
