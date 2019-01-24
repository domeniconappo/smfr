#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${DIR}/export_env.sh

${DIR}/singlenode_dbs.sh
echo "Waiting for DBs to bootstrap: 45 seconds"
sleep 45

python ${DIR}/update_smfr_nuts_tables.py

echo "DBs were started locally. Execute ./singlenode_down.sh to shut down"