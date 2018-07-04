#!/usr/bin/env bash

command=${1:-1}

PROPERTY_FILE=.env

function getProperty {
   PROP_KEY=$1
   PROP_VALUE=`cat ${PROPERTY_FILE} | grep "$PROP_KEY" | cut -d'=' -f2`
   echo ${PROP_VALUE}
}

export current_branch=`git rev-parse --symbolic-full-name --abbrev-ref HEAD`
SMFR_DATADIR=$(getProperty "SMFR_DATADIR")
GIT_REPO_MODELS=$(getProperty "GIT_REPO_MODELS")
DOCKER_ID_USER=$(getProperty "DOCKER_ID_USER")

if [ ! -d ${SMFR_DATADIR} ]; then
    mkdir -p ${SMFR_DATADIR}
fi

if [ ! -d ${SMFR_DATADIR}/geonames_index ]; then
    # Download geonames indices for geocoding
    cd ${SMFR_DATADIR}
    wget https://s3.amazonaws.com/ahalterman-geo/geonames_index.tar.gz
    tar xzf geonames_index.tar.gz
    rm geonames_index.tar.gz

    chown -R systemd-resolve:systemd-timesync geonames_index
    cd -
fi

docker build --build-arg http_proxy=${http_proxy} --build-arg https_proxy=${http_proxy} -t smfr_base:${current_branch} base_docker/.
docker tag smfr_base:${current_branch} efas/smfr_base:${current_branch}

if [ -n "${DOCKER_ID_USER}" ] && [ ${command} == "push" ]; then
    docker push efas/smfr_base:${current_branch}
fi


python3 compose4build.py ${current_branch}
docker-compose build

if [ -n "${DOCKER_ID_USER}" ] && [ ${command} == "push" ]; then
    docker tag efas/persister:${current_branch} efas/persister:${current_branch}
    docker tag efas/annotator:${current_branch} efas/annotator:${current_branch}
    docker tag efas/geocoder:${current_branch} efas/geocoder:${current_branch}
    docker tag efas/restserver:${current_branch} efas/restserver:${current_branch}
    docker tag efas/web:${current_branch} efas/web:${current_branch}

    docker push efas/persister:${current_branch}
    docker push efas/annotator:${current_branch}
    docker push efas/geocoder:${current_branch}
    docker push efas/restserver:${current_branch}
    docker push efas/web:${current_branch}
fi
