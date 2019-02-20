#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${DIR}/functions.sh

export image_tag=`cat ${DIR}/../VERSION`

echo
echo
echo -------------------------- -------------------------- -------------------------- --------------------------
echo "                                        Bootstrapping SMFR VERSION ${image_tag}"
echo -------------------------- -------------------------- -------------------------- --------------------------
echo
echo
DOCKER_ID_USER=$(getProperty "DOCKER_ID_USER")
DOCKER_ID_PASSWORD=$(getProperty "DOCKER_ID_PASSWORD")
DOCKER_REGISTRY=$(getProperty "DOCKER_REGISTRY")

if [[ -n "${DOCKER_ID_USER}" ]] && [[ ${DOCKER_REGISTRY} != "index.docker.io" ]]; then
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
PRODUCTS_IMAGE=$(getProperty "PRODUCTS_IMAGE")
KAFKA_IMAGE=$(getProperty "KAFKA_IMAGE")
ZOOKEEPER_IMAGE=$(getProperty "ZOOKEEPER_IMAGE")

docker-compose -f ${DIR}/../docker-compose.yaml -f ${DIR}/../docker-compose.dbs.yaml config > ${DIR}/../docker-compose-parsed.yaml

# cleaning volumes from docker compose configuration
python3 ${DIR}/compose4deploy.py -i ${DIR}/../docker-compose-parsed.yaml -o ${DIR}/../docker-compose-4deploy.yaml

docker stack deploy --with-registry-auth -c ${DIR}/../docker-compose-4deploy.yaml SMFR -c ${DIR}/../docker-compose.swarm.yaml -c ${DIR}/../docker-compose.dbs.swarm.yaml

# forcing updates of images
docker service update SMFR_mysql --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${MYSQL_IMAGE}:${image_tag}
docker service update SMFR_cassandrasmfr --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${CASSANDRA_IMAGE}:${image_tag}
docker service update SMFR_geonames --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${GEONAMES_IMAGE}:${image_tag}

docker service update SMFR_persister --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${PERSISTER_IMAGE}:${image_tag}
docker service update SMFR_annotator --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${ANNOTATOR_IMAGE}:${image_tag}
docker service update SMFR_geocoder --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${GEOCODER_IMAGE}:${image_tag}
docker service update SMFR_aggregator --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${AGGREGATOR_IMAGE}:${image_tag}
docker service update SMFR_products --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${PRODUCTS_IMAGE}:${image_tag}
docker service update SMFR_restserver --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${RESTSERVER_IMAGE}:${image_tag}
docker service update SMFR_web --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${WEB_IMAGE}:${image_tag}
