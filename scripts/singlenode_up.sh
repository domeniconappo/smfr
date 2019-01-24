#!/usr/bin/env bash
export SINGLENODE=1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
service=${1:-web}
export image_tag=`cat ${DIR}/../VERSION`
python3 ${DIR}/compose4build.py ${image_tag}
echo
echo
echo ++++++++++++++++++++++++++ Starting ${service} and its dependent services ++++++++++++++++++++++++++
docker-compose -f ${DIR}/../docker-compose.yaml -f ${DIR}/../docker-compose.dbs.yaml up -d ${service}
