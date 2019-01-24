#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${DIR}/export_env.sh
export SINGLENODE=1
export image_tag=`cat ./VERSION`
python3 ${DIR}/compose4build.py ${image_tag}
echo
echo
echo ++++++++++++++++++++++++++ Starting DBS ++++++++++++++++++++++++++
docker-compose -f ${DIR}/../docker-compose.dbs.yaml up -d mysql cassandrasmfr
