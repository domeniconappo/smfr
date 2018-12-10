#!/usr/bin/env bash

source functions.sh

mode=${1:-compose}
if [[ ${mode} == "compose" ]]; then
./singlenode_down.sh
prefix="smfr"
elif [[ ${mode} == "swarm" ]]; then
prefix="SMFR"
./swarm_down.sh
fi

DOCKER_REGISTRY=$(getProperty "DOCKER_REGISTRY")
BACKUPPER_IMAGE=$(getProperty "BACKUPPER_IMAGE")

sleep 20

echo "--------- Start Backups"
echo "--------- MySQL"
docker run -e LOCAL_USER_ID=`id -u ${USER}` -v ${prefix}_vlm-mysql:/volume -v /tmp:/backup --rm ${DOCKER_REGISTRY}/${BACKUPPER_IMAGE} backup mysql
echo "[OK] MySQL"
echo "--------- CASSANDRA"
docker run -e LOCAL_USER_ID=`id -u ${USER}` -v ${prefix}_vlm-cassandra:/volume -v /tmp:/backup --rm ${DOCKER_REGISTRY}/${BACKUPPER_IMAGE} backup cassandra
echo "[OK] CASSANDRA"
sleep 20

if [[ ${mode} == "compose" ]]; then
./singlenode_up.sh
elif [[ ${mode} == "swarm" ]]; then
./swarm_up.sh
fi
