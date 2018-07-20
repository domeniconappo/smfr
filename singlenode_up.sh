#!/usr/bin/env bash
service=${1:web}
current_branch=`git rev-parse --symbolic-full-name --abbrev-ref HEAD`
if [ ${current_branch} == "master" ]; then
    image_tag='latest'
else
    image_tag=`cat VERSION | grep "VERSION" | cut -d'=' -f2`
fi
export image_tag
python3 compose4build.py ${image_tag}
echo ++++++++++++++++++++++++++ Starting ${service} and its dependent services ++++++++++++++++++++++++++
docker-compose up -d ${service}
