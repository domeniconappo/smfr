#!/usr/bin/env bash

command=${1:-1}

PROPERTY_FILE=.env
SERVICES="web restserver geocoder annotator persister aggregator cassandrasmfr mysql products"
logged=0

function getProperty {
   PROP_KEY=$1
   PROP_VALUE=`cat ${PROPERTY_FILE} | grep -v "#${PROP_KEY}" | grep "${PROP_KEY}" | cut -d'=' -f2`
   echo ${PROP_VALUE}
}

export image_tag=`cat VERSION | grep "VERSION" | cut -d'=' -f2`

SMFR_DATADIR=$(getProperty "SMFR_DATADIR")
GIT_REPO_MODELS=$(getProperty "GIT_REPO_MODELS")
DOCKER_ID_USER=$(getProperty "DOCKER_ID_USER")
DOCKER_ID_PASSWORD=$(getProperty "DOCKER_ID_PASSWORD")
DOCKER_REGISTRY=$(getProperty "DOCKER_REGISTRY")
SMFR_IMAGE=$(getProperty "SMFR_IMAGE")

if [ -n "${DOCKER_ID_USER}" ] && [ ${DOCKER_REGISTRY} != "index.docker.io" ]; then
    echo Pulling from a private registry: ${DOCKER_REGISTRY} - need to login
    docker login -u ${DOCKER_ID_USER} -p ${DOCKER_ID_PASSWORD} ${DOCKER_REGISTRY}
    logged=1
fi
if [ ! -d ${SMFR_DATADIR} ]; then
    mkdir -p ${SMFR_DATADIR}
fi

# building with docker-compose
python3 compose4build.py ${image_tag}

# build base image
docker build --build-arg http_proxy=${http_proxy} --build-arg https_proxy=${http_proxy} -t smfr_base:${image_tag} base/.
docker tag smfr_base:${image_tag} ${SMFR_IMAGE}:${image_tag}

# push base image
if [ -n "${DOCKER_ID_USER}" ] && [ ${command} == "push" ]; then
    if  [ ${logged} == 0 ]; then
        docker login -u ${DOCKER_ID_USER} -p ${DOCKER_ID_PASSWORD} ${DOCKER_REGISTRY}
    fi
    docker tag ${SMFR_IMAGE}:${image_tag} ${DOCKER_REGISTRY}/${SMFR_IMAGE}:${image_tag}
    docker push ${DOCKER_REGISTRY}/${SMFR_IMAGE}:${image_tag}
fi


if [ -n "`echo ${SERVICES} | xargs -n1 echo | grep ${command}`" ]; then
    echo  ++++++++++++++++++++ Building ${command} service +++++++++++++++++++++++++++++++
    docker-compose build ${command}
else
    echo !!! Building all services !!!
    docker-compose build
fi

# push images

if [ -n "${DOCKER_ID_USER}" ] && [ ${command} == "push" ]; then
    PERSISTER_IMAGE=$(getProperty "PERSISTER_IMAGE")
    AGGREGATOR_IMAGE=$(getProperty "AGGREGATOR_IMAGE")
    ANNOTATOR_IMAGE=$(getProperty "ANNOTATOR_IMAGE")
    GEOCODER_IMAGE=$(getProperty "GEOCODER_IMAGE")
    RESTSERVER_IMAGE=$(getProperty "RESTSERVER_IMAGE")
    WEB_IMAGE=$(getProperty "WEB_IMAGE")
    MYSQL_IMAGE=$(getProperty "MYSQL_IMAGE")
    CASSANDRA_IMAGE=$(getProperty "CASSANDRA_IMAGE")
    GEONAMES_IMAGE=$(getProperty "GEONAMES_IMAGE")
    PRODUCTS_IMAGE=$(getProperty "PRODUCTS_IMAGE")

    echo !!! Pushing images to ${DOCKER_REGISTRY} as user ${DOCKER_ID_USER}!!!
    echo
    docker tag ${PERSISTER_IMAGE}:${image_tag} ${DOCKER_REGISTRY}/${PERSISTER_IMAGE}:${image_tag}
    docker tag ${AGGREGATOR_IMAGE}:${image_tag} ${DOCKER_REGISTRY}/${AGGREGATOR_IMAGE}:${image_tag}
    docker tag ${ANNOTATOR_IMAGE}:${image_tag} ${DOCKER_REGISTRY}/${ANNOTATOR_IMAGE}:${image_tag}
    docker tag ${GEOCODER_IMAGE}:${image_tag} ${DOCKER_REGISTRY}/${GEOCODER_IMAGE}:${image_tag}
    docker tag ${PRODUCTS_IMAGE}:${image_tag} ${DOCKER_REGISTRY}/${PRODUCTS_IMAGE}:${image_tag}
    docker tag ${RESTSERVER_IMAGE}:${image_tag} ${DOCKER_REGISTRY}/${RESTSERVER_IMAGE}:${image_tag}
    docker tag ${WEB_IMAGE}:${image_tag} ${DOCKER_REGISTRY}/${WEB_IMAGE}:${image_tag}
    docker tag ${MYSQL_IMAGE}:${image_tag} ${DOCKER_REGISTRY}/${MYSQL_IMAGE}:${image_tag}
    docker tag ${CASSANDRA_IMAGE}:${image_tag} ${DOCKER_REGISTRY}/${CASSANDRA_IMAGE}:${image_tag}
    docker tag ${GEONAMES_IMAGE}:${image_tag} ${DOCKER_REGISTRY}/${GEONAMES_IMAGE}:${image_tag}

    docker push ${DOCKER_REGISTRY}/${PERSISTER_IMAGE}:${image_tag}
    docker push ${DOCKER_REGISTRY}/${AGGREGATOR_IMAGE}:${image_tag}
    docker push ${DOCKER_REGISTRY}/${ANNOTATOR_IMAGE}:${image_tag}
    docker push ${DOCKER_REGISTRY}/${GEOCODER_IMAGE}:${image_tag}
    docker push ${DOCKER_REGISTRY}/${PRODUCTS_IMAGE}:${image_tag}
    docker push ${DOCKER_REGISTRY}/${RESTSERVER_IMAGE}:${image_tag}
    docker push ${DOCKER_REGISTRY}/${WEB_IMAGE}:${image_tag}
    docker push ${DOCKER_REGISTRY}/${MYSQL_IMAGE}:${image_tag}
    docker push ${DOCKER_REGISTRY}/${GEONAMES_IMAGE}:${image_tag}
    docker push ${DOCKER_REGISTRY}/${CASSANDRA_IMAGE}:${image_tag}
fi
