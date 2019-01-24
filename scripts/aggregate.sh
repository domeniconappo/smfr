#!/usr/bin/env bash

# A command to launch python script to aggregate a given collection
#
# Usage:
# aggregate.sh -c 42
# aggregate.sh --collection_id 42
# aggregate.sh -r
# aggregate.sh --running
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${DIR}/export_env.sh
python3 ${DIR}/aggregate.py $@
