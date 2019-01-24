#!/usr/bin/env bash

# A command to launch python script to execute classification and geolocalization for a given collection
#
# Usage:
# reannotate.sh -c 42
# reannotate.sh --collection_id 42
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${DIR}/export_env.sh

python3 ${DIR}/reannotate.py $@
