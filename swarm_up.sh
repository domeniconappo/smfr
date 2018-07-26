#!/usr/bin/env bash

PROPERTY_FILE=.env

function getProperty {
   PROP_KEY=$1
   PROP_VALUE=`cat ${PROPERTY_FILE} | grep -v "#${PROP_KEY}" | grep "${PROP_KEY}" | cut -d'=' -f2`
   echo ${PROP_VALUE}
}

export image_tag=`cat VERSION | grep "VERSION" | cut -d'=' -f2`

docker-compose config > docker-compose-parsed.yaml

# cleaning volumes from docker compose configuration
python3 compose4deploy.py -i docker-compose-parsed.yaml -o docker-compose-4deploy.yaml
docker stack deploy -c ./docker-compose-4deploy.yaml SMFR

# forcing updates of images
PERSISTER_IMAGE=$(getProperty "PERSISTER_IMAGE")
AGGREGATOR_IMAGE=$(getProperty "AGGREGATOR_IMAGE")
ANNOTATOR_IMAGE=$(getProperty "ANNOTATOR_IMAGE")
GEOCODER_IMAGE=$(getProperty "GEOCODER_IMAGE")
RESTSERVER_IMAGE=$(getProperty "RESTSERVER_IMAGE")
WEB_IMAGE=$(getProperty "WEB_IMAGE")
MYSQL_IMAGE=$(getProperty "MYSQL_IMAGE")
CASSANDRA_IMAGE=$(getProperty "CASSANDRA_IMAGE")
GEONAMES_IMAGE=$(getProperty "GEONAMES_IMAGE")

docker service update SMFR_persister --detach=false --image ${PERSISTER_IMAGE}:${image_tag}
docker service update SMFR_geonames --detach=false --image ${AGGREGATOR_IMAGE}:${image_tag}
docker service update SMFR_mysql --detach=false --image ${ANNOTATOR_IMAGE}:${image_tag}
docker service update SMFR_annotator --detach=false --image ${GEOCODER_IMAGE}:${image_tag}
docker service update SMFR_geocoder --detach=false --image ${RESTSERVER_IMAGE}:${image_tag}
docker service update SMFR_restserver --detach=false --image ${WEB_IMAGE}:${image_tag}
docker service update SMFR_web --detach=false --image ${MYSQL_IMAGE}:${image_tag}
docker service update SMFR_cassandrasmfr --detach=false --image ${CASSANDRA_IMAGE}:${image_tag}
docker service update SMFR_geonames --detach=false --image ${GEONAMES_IMAGE}:${image_tag}
