#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source ${DIR}/functions.sh

command=${1:-1}
second_command=${2:-1}

SERVICES="web restserver geocoder annotator persister aggregator cassandrasmfr mysql products"
logged=0

echo
export image_tag=`cat ./VERSION`
echo =================================================== Building SMFR ${image_tag}
echo
echo

GIT_REPO_MODELS=$(getProperty "GIT_REPO_MODELS")
DOCKER_ID_USER=$(getProperty "DOCKER_ID_USER")
DOCKER_ID_PASSWORD=$(getProperty "DOCKER_ID_PASSWORD")
DOCKER_REGISTRY=$(getProperty "DOCKER_REGISTRY")
SMFR_IMAGE=$(getProperty "SMFR_IMAGE")
BACKUPPER_IMAGE=$(getProperty "BACKUPPER_IMAGE")

if [[ -n "${DOCKER_ID_USER}" ]] && [[ ${DOCKER_REGISTRY} != "index.docker.io" ]]; then
    echo Pulling from a private registry: ${DOCKER_REGISTRY} - need to login
    docker login -u ${DOCKER_ID_USER} -p ${DOCKER_ID_PASSWORD} ${DOCKER_REGISTRY}
    logged=1
fi

python3 ${DIR}/compose4build.py ${image_tag}

# build base image
docker build --build-arg http_proxy=${http_proxy} --build-arg https_proxy=${http_proxy} -t ${SMFR_IMAGE}:${image_tag} base/.
echo
echo

# build backup image
docker build --build-arg http_proxy=${http_proxy} --build-arg https_proxy=${http_proxy} -t ${BACKUPPER_IMAGE}:latest backupper/.
echo
echo

# push core images always
if [[ -n "${DOCKER_ID_USER}" ]]; then
    if  [[ ${logged} == 0 ]]; then
        docker login -u ${DOCKER_ID_USER} -p ${DOCKER_ID_PASSWORD} ${DOCKER_REGISTRY}
    fi
    docker tag ${SMFR_IMAGE}:${image_tag} ${DOCKER_REGISTRY}/${SMFR_IMAGE}:${image_tag}
    docker push ${DOCKER_REGISTRY}/${SMFR_IMAGE}:${image_tag}

    docker tag ${BACKUPPER_IMAGE}:latest ${DOCKER_REGISTRY}/${BACKUPPER_IMAGE}:latest
    docker push ${DOCKER_REGISTRY}/${BACKUPPER_IMAGE}:latest
fi

echo
echo

# Set VERSION for shared libraries
cp ${DIR}/../VERSION base/shared_libs/smfr_models/
cp ${DIR}/../VERSION base/shared_libs/smfr_clients/
cp ${DIR}/../VERSION base/shared_libs/smfr_utils/
cp ${DIR}/../VERSION base/shared_libs/smfr_analysis/

if [[ -n "`echo ${SERVICES} | xargs -n1 echo | grep ${command}`" ]]; then
    echo  ++++++++++++++++++++ Building ${command} service +++++++++++++++++++++++++++++++
    docker-compose -f ${DIR}/../docker-compose.dbs.yaml -f ${DIR}/../docker-compose.yaml build ${command}
else
    echo !!! Building all services !!!
    docker-compose -f ${DIR}/../docker-compose.dbs.yaml -f ${DIR}/../docker-compose.yaml build
fi

# push images
echo
echo
if [[ -n "${DOCKER_ID_USER}" ]] && [[ ${command} == "push" ]]; then
    echo !!! Pushing images to ${DOCKER_REGISTRY} as user ${DOCKER_ID_USER}!!!
    echo

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
