#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROPERTY_FILE=${DIR}/../.env

function getProperty {
   PROP_KEY=$1
   PROP_VALUE=`cat ${PROPERTY_FILE} | grep -v "#${PROP_KEY}" | grep "${PROP_KEY}" | cut -d'=' -f2`
   echo ${PROP_VALUE}
}
