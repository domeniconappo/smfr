#!/usr/bin/env bash
export SINGLENODE=1
service=${1:-web}
export image_tag=`cat ./VERSION`
python3 scripts/compose4build.py ${image_tag}
echo
echo
echo ++++++++++++++++++++++++++ Starting ${service} and its dependent services ++++++++++++++++++++++++++
docker-compose -f docker-compose.yaml -f docker-compose.dbs.yaml up -d ${service}
