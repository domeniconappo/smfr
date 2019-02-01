#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export image_tag=`cat ${DIR}/../VERSION`
export SINGLENODE=1

${DIR}/singlenode_down.sh
#${DIR}/build.sh tester

python3 ${DIR}/compose4build.py ${image_tag}

echo
echo
echo ++++++++++++++++++++++++++ Starting tester and its dependent services ++++++++++++++++++++++++++
docker-compose -f ${DIR}/../docker-compose.yaml -f ${DIR}/../docker-compose.dbs.yaml -f ${DIR}/../docker-compose.test.yaml up tester

${DIR}/singlenode_down.sh
