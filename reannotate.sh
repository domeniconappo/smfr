#!/usr/bin/env bash

# usage
# reannotate.sh -c 42 -t annotated -l en
# reannotate.sh --collection_id 42 --ttype annotated --lang en
# defaults: ttype: annotated; lang: None (all available languages)
source export_env.sh
python3 scripts/reannotate.py $@
