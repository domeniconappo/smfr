#!/usr/bin/env bash

PROPERTY_FILE=.env

function getProperty {
   PROP_KEY=$1
   PROP_VALUE=`cat ${PROPERTY_FILE} | grep -v "#${PROP_KEY}" | grep "${PROP_KEY}" | cut -d'=' -f2`
   echo ${PROP_VALUE}
}

export image_tag=`cat VERSION | grep "VERSION" | cut -d'=' -f2`


DOCKER_ID_USER=$(getProperty "DOCKER_ID_USER")
DOCKER_ID_PASSWORD=$(getProperty "DOCKER_ID_PASSWORD")
DOCKER_REGISTRY=$(getProperty "DOCKER_REGISTRY")

if [ -n "${DOCKER_ID_USER}" ] && [ ${DOCKER_REGISTRY} != "index.docker.io" ]; then
    echo Pulling from a private registry: ${DOCKER_REGISTRY} - need to login
    docker login -u ${DOCKER_ID_USER} -p ${DOCKER_ID_PASSWORD} ${DOCKER_REGISTRY}
fi

PERSISTER_IMAGE=$(getProperty "PERSISTER_IMAGE")
AGGREGATOR_IMAGE=$(getProperty "AGGREGATOR_IMAGE")
ANNOTATOR_IMAGE=$(getProperty "ANNOTATOR_IMAGE")
GEOCODER_IMAGE=$(getProperty "GEOCODER_IMAGE")
RESTSERVER_IMAGE=$(getProperty "RESTSERVER_IMAGE")
WEB_IMAGE=$(getProperty "WEB_IMAGE")
MYSQL_IMAGE=$(getProperty "MYSQL_IMAGE")
CASSANDRA_IMAGE=$(getProperty "CASSANDRA_IMAGE")
GEONAMES_IMAGE=$(getProperty "GEONAMES_IMAGE")

docker-compose config > docker-compose-parsed.yaml

# cleaning volumes from docker compose configuration
python3 compose4deploy.py -i docker-compose-parsed.yaml -o docker-compose-4deploy.yaml

docker stack deploy --with-registry-auth -c ./docker-compose-4deploy.yaml SMFR

# forcing updates of images

docker service update SMFR_mysql --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${MYSQL_IMAGE}:${image_tag}
docker service update SMFR_cassandrasmfr --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${CASSANDRA_IMAGE}:${image_tag}
docker service update SMFR_geonames --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${GEONAMES_IMAGE}:${image_tag}

docker service update SMFR_persister --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${PERSISTER_IMAGE}:${image_tag}
docker service update SMFR_annotator --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${ANNOTATOR_IMAGE}:${image_tag}
docker service update SMFR_geocoder --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${GEOCODER_IMAGE}:${image_tag}
docker service update SMFR_aggregator --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${AGGREGATOR_IMAGE}:${image_tag}

docker service update SMFR_restserver --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${RESTSERVER_IMAGE}:${image_tag}
docker service update SMFR_web --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${WEB_IMAGE}:${image_tag}
