#!/usr/bin/env bash

# usage
# reannotate.sh -c 42 -t annotated
# reannotate.sh --collection_id 42 --ttype annotated

# defaults: ttype: annotated
source export_env.sh

python3 scripts/reannotate.py $@
