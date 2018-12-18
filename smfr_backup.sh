#!/usr/bin/env bash

source functions.sh

mode=${1:-compose}
echo " ----------------------------------------------------- "
echo "Backup SMFR databases. Shut down SMFR first if needed..."
echo " ----------------------------------------------------- "

if [[ ${mode} == "compose" ]]; then
    ./singlenode_down.sh
    prefix="smfr"
elif [[ ${mode} == "swarm" ]]; then
    prefix="SMFR"
    ./swarm_down.sh
fi

DOCKER_REGISTRY=$(getProperty "DOCKER_REGISTRY")
BACKUPPER_IMAGE=$(getProperty "BACKUPPER_IMAGE")
BACKUP_FOLDER=$(getProperty "BACKUP_FOLDER")

echo "CONFIG:"
echo "-------"

echo "DOCKER REGISTRY ${DOCKER_REGISTRY}"
echo "BACKUPPER IMAGE ${BACKUPPER_IMAGE}:latest"
echo "BACKUP FOLDER ${BACKUP_FOLDER}"
echo
echo

sleep 20
echo "--------- Start Backups"
echo
echo
echo "--------- MySQL"
docker run -v ${prefix}_vlm-mysql:/volume -v ${BACKUP_FOLDER}:/backup --rm ${DOCKER_REGISTRY}/${BACKUPPER_IMAGE}:latest /volume-backup.sh backup mysql
echo "[OK] MySQL"
echo "--------- CASSANDRA"
docker run -v ${prefix}_vlm-cassandra:/volume -v ${BACKUP_FOLDER}:/backup --rm ${DOCKER_REGISTRY}/${BACKUPPER_IMAGE}:latest /volume-backup.sh backup cassandra
echo "[OK] CASSANDRA"

if [[ ${mode} == "compose" ]]; then
    ./singlenode_up.sh
    prefix="smfr"
elif [[ ${mode} == "swarm" ]]; then
    prefix="SMFR"
    ./swarm_up.sh
fi