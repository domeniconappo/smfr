#!/usr/bin/env bash
source export_env.sh
export SINGLENODE=1
export image_tag=`cat ./VERSION`
python3 scripts/compose4build.py ${image_tag}
echo
echo
echo ++++++++++++++++++++++++++ Starting DBS ++++++++++++++++++++++++++
docker-compose -f docker-compose-dbs.yaml up -d mysql cassandrasmfr
