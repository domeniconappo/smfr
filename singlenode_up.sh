#!/usr/bin/env bash
service=${1:-web}
export image_tag=`cat VERSION | grep "VERSION" | cut -d'=' -f2`
python3 compose4build.py ${image_tag}
echo ++++++++++++++++++++++++++ Starting ${service} and its dependent services ++++++++++++++++++++++++++
docker-compose up -d ${service}
