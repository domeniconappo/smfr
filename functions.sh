#!/usr/bin/env bash

PROPERTY_FILE=.env

function getProperty {
   PROP_KEY=$1
   PROP_VALUE=`cat ${PROPERTY_FILE} | grep -v "#${PROP_KEY}" | grep "${PROP_KEY}" | cut -d'=' -f2`
   echo ${PROP_VALUE}
}
