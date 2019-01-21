#!/usr/bin/env bash

source functions.sh

export image_tag=`cat ./VERSION`

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

docker-compose -f docker-compose-dbs.yaml config > docker-compose-parsed-dbs.yaml

# cleaning volumes from docker compose configuration
python3 scripts/compose4deploy.py -i docker-compose-parsed-dbs.yaml -o docker-compose-4deploy-dbs.yaml

docker stack deploy --with-registry-auth -c ./docker-compose-4deploy-dbs.yaml SMFR -c ./docker-compose.swarm.yaml

# forcing updates of images

docker service update SMFR_mysql --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${MYSQL_IMAGE}:${image_tag}
docker service update SMFR_cassandrasmfr --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${CASSANDRA_IMAGE}:${image_tag}
docker service update SMFR_geonames --detach=false --with-registry-auth --image ${DOCKER_REGISTRY}/${GEONAMES_IMAGE}:${image_tag}
