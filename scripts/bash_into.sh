#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${DIR}/export_env.sh
service=${1}
mode=${2:-compose}
echo " ----------------------------------------------------- "
echo "              Login to ${service} in ${mode} mode    "
echo " ----------------------------------------------------- "

if [[ ${mode} == "swarm" ]]; then
    container=$(docker ps --filter label=com.docker.swarm.service.name=SMFR_${service} | awk '{if (NR!=1) {print $1}}')
fi
if [[ -z ${container} ]]; then
    echo "Container is not running: ${service}"
else
    echo "docker exec -it ${container} bash"
    docker exec -it ${container} bash
fi
