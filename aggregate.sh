#!/usr/bin/env bash

# A command to launch python script to aggregate a given collection
#
# Usage:
# aggregate.sh -c 42
# aggregate.sh --collection_id 42
# aggregate.sh -r
# aggregate.sh --running

source export_env.sh

python3 scripts/aggregate.py $@
