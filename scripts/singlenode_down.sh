#!/usr/bin/env bash
export SINGLENODE=0
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
docker-compose -f ${DIR}/../docker-compose.dbs.yaml -f ${DIR}/../docker-compose.yaml stop
