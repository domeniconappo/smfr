#!/usr/bin/env bash
export SINGLENODE=1
service=${1:-web}
export image_tag=`cat VERSION | grep "VERSION" | cut -d'=' -f2`
python3 scripts/compose4build.py ${image_tag}
echo
echo
echo ++++++++++++++++++++++++++ Starting ${service} and its dependent services ++++++++++++++++++++++++++
docker-compose up -d ${service}
