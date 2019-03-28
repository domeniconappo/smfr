#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${DIR}/export_env.sh
mode=${1:-compose}
echo " ----------------------------------------------------- "
echo "              CQLSH in ${mode} mode                    "
echo " ----------------------------------------------------- "

if [[ ${mode} == "compose" ]]; then
    container="cassandrasmfr"
elif [[ ${mode} == "swarm" ]]; then
    container=$(docker ps --filter label=com.docker.swarm.service.name=SMFR_cassandrasmfr | awk '{if (NR!=1) {print $1}}')
fi
echo "docker exec -it ${container} cqlsh -u ${CASSANDRA_USER} (insert C* password when required)"
docker exec -it ${container} cqlsh -u ${CASSANDRA_USER} --request-timeout 36000
