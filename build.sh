#!/usr/bin/env bash

PROPERTY_FILE=.env

function getProperty {
   PROP_KEY=$1
   PROP_VALUE=`cat ${PROPERTY_FILE} | grep "$PROP_KEY" | cut -d'=' -f2`
   echo ${PROP_VALUE}
}


SMFR_DATADIR=$(getProperty "SMFR_DATADIR")

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

if [ -n "${DOCKER_ID_USER}" ]; then

    docker build --build-arg http_proxy=${http_proxy} --build-arg https_proxy=${http_proxy} -t smfr_base base_docker/.
    docker tag smfr_base efas/smfr_base
    docker push efas/smfr_base
fi
docker-compose build
