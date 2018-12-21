#!/usr/bin/env bash

# A command to launch python script to execute classification and geolocalization for a given collection
#
# Usage:
# reannotate.sh -c 42
# reannotate.sh --collection_id 42

source export_env.sh

python3 scripts/reannotate.py $@
