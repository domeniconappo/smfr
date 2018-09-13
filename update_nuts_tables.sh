#!/usr/bin/env bash
source export_env.sh

./singlenode_up.sh mysql
echo "Waiting for mysql bootstrap: 10 seconds"
sleep 10

python scripts/update_smfr_nuts_tables.py

./singlenode_down.sh
