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
    image_tag='latest'
else
    image_tag=`cat VERSION | grep "VERSION" | cut -d'=' -f2`
fi
export image_tag

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
docker build --build-arg http_proxy=${http_proxy} --build-arg https_proxy=${http_proxy} -t smfr_base:${image_tag} base_docker/.
docker tag smfr_base:${image_tag} efas/smfr_base:${image_tag}

# push base image
if [ -n "${DOCKER_ID_USER}" ] && [ ${command} == "push" ]; then
    docker push efas/smfr_base:${image_tag}
fi


# building with docker-compose
python3 compose4build.py ${image_tag}

if [ -n "`echo ${SERVICES} | xargs -n1 echo | grep ${command}`" ]; then
    echo  ++++++++++++++++++++ Building ${command} service +++++++++++++++++++++++++++++++
    docker-compose build ${command}
else
    echo !!! Building all services !!!
    docker-compose build
fi

# push images

if [ -n "${DOCKER_ID_USER}" ] && [ ${command} == "push" ]; then
    docker tag efas/persister:${image_tag} efas/persister:${image_tag}
    docker tag efas/annotator:${image_tag} efas/annotator:${image_tag}
    docker tag efas/geocoder:${image_tag} efas/geocoder:${image_tag}
    docker tag efas/restserver:${image_tag} efas/restserver:${image_tag}
    docker tag efas/web:${image_tag} efas/web:${image_tag}
    docker tag efas/mysql:${image_tag} efas/mysql:${image_tag}
    docker tag efas/geonames:${image_tag} efas/geonames:${image_tag}

    docker push efas/persister:${image_tag}
    docker push efas/annotator:${image_tag}
    docker push efas/geocoder:${image_tag}
    docker push efas/restserver:${image_tag}
    docker push efas/web:${image_tag}
    docker push efas/mysql:${image_tag}
    docker push efas/geonames:${image_tag}
fi
