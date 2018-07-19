#!/usr/bin/env bash

command=${1:-1}

PROPERTY_FILE=.env
SERVICES="web restserver geocoder annotator persister aggregator"
function getProperty {
   PROP_KEY=$1
   PROP_VALUE=`cat ${PROPERTY_FILE} | grep "$PROP_KEY" | cut -d'=' -f2`
   echo ${PROP_VALUE}
}

current_branch=`git rev-parse --symbolic-full-name --abbrev-ref HEAD`
if [ ${current_branch} == "master" ]; then
    current_branch='latest'
fi
export current_branch

SMFR_DATADIR=$(getProperty "SMFR_DATADIR")
GIT_REPO_MODELS=$(getProperty "GIT_REPO_MODELS")
DOCKER_ID_USER=$(getProperty "DOCKER_ID_USER")

if [ ! -d ${SMFR_DATADIR} ]; then
    mkdir -p ${SMFR_DATADIR}
fi

# update geonames ES index from https://s3.amazonaws.com/ahalterman-geo/
if [ ! -d ${SMFR_DATADIR}/geonames_index ] || [ ${command} == "update_index" ]; then
    # Download geonames indices for geocoding
    cd ${SMFR_DATADIR}
    wget https://s3.amazonaws.com/ahalterman-geo/geonames_index.tar.gz
    tar xzf geonames_index.tar.gz
    rm geonames_index.tar.gz
    cd -
fi

# build base image
docker build --build-arg http_proxy=${http_proxy} --build-arg https_proxy=${http_proxy} -t smfr_base:${current_branch} base_docker/.
docker tag smfr_base:${current_branch} efas/smfr_base:${current_branch}

# push base image
if [ -n "${DOCKER_ID_USER}" ] && [ ${command} == "push" ]; then
    docker push efas/smfr_base:${current_branch}
fi


# building with docker-compose
python3 compose4build.py ${current_branch}

if [ -n "`echo ${SERVICES} | xargs -n1 echo | grep ${command}`" ]; then
    echo  ++++++++++++++++++++ Building ${command} service +++++++++++++++++++++++++++++++
    docker-compose build ${command}
else
    echo Building all services
    docker-compose build
fi

# push images

if [ -n "${DOCKER_ID_USER}" ] && [ ${command} == "push" ]; then
    docker tag efas/persister:${current_branch} efas/persister:${current_branch}
    docker tag efas/annotator:${current_branch} efas/annotator:${current_branch}
    docker tag efas/geocoder:${current_branch} efas/geocoder:${current_branch}
    docker tag efas/restserver:${current_branch} efas/restserver:${current_branch}
    docker tag efas/web:${current_branch} efas/web:${current_branch}
    docker tag efas/mysql:latest efas/mysql:latest
    docker tag efas/geonames:latest efas/geonames:latest

    docker push efas/persister:${current_branch}
    docker push efas/annotator:${current_branch}
    docker push efas/geocoder:${current_branch}
    docker push efas/restserver:${current_branch}
    docker push efas/web:${current_branch}
    docker push efas/mysql:latest
    docker push efas/geonames:latest
fi
