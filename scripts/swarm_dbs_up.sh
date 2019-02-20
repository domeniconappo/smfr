#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${DIR}/functions.sh

export image_tag=`cat ${DIR}/../VERSION`

echo
echo
echo -------------------------- -------------------------- -------------------------- --------------------------
echo "                                        Bootstrapping SMFR DBS VERSION ${image_tag}"
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

MYSQL_IMAGE=$(getProperty "MYSQL_IMAGE")
CASSANDRA_IMAGE=$(getProperty "CASSANDRA_IMAGE")
GEONAMES_IMAGE=$(getProperty "GEONAMES_IMAGE")
KAFKA_IMAGE=$(getProperty "KAFKA_IMAGE")
ZOOKEEPER_IMAGE=$(getProperty "ZOOKEEPER_IMAGE")

docker-compose -f ${DIR}/../docker-compose.dbs.yaml config > ${DIR}/../docker-compose-parsed-dbs.yaml

# cleaning volumes from docker compose configuration
python3 ${DIR}/compose4deploy.py -i ${DIR}/../docker-compose-parsed-dbs.yaml -o ${DIR}/../docker-compose-4deploy-dbs.yaml

docker stack deploy --with-registry-auth -c ${DIR}/../docker-compose-4deploy-dbs.yaml SMFR -c ${DIR}/../docker-compose.dbs.swarm.yaml

# forcing updates of images
docker service update SMFR_mysql --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${MYSQL_IMAGE}:${image_tag}
docker service update SMFR_cassandrasmfr --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${CASSANDRA_IMAGE}:${image_tag}
docker service update SMFR_geonames --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${GEONAMES_IMAGE}:${image_tag}
