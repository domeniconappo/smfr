#!/usr/bin/env bash
source export_env.sh

./singlenode_up.sh mysql
./singlenode_up.sh cassandrasmfr
echo "Waiting for DBs to bootstrap: 45 seconds"
sleep 45

python scripts/update_smfr_nuts_tables.py

echo "DBs were started locally. Execute ./singlenode_down.sh to shut down"