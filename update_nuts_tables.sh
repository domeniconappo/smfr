#!/usr/bin/env bash
source functions.sh

./singlenode_up.sh mysql

export MYSQL_USER=$(getProperty "MYSQL_USER")
export MYSQL_PASSWORD=$(getProperty "MYSQL_PASSWORD")
export MYSQL_DBNAME=$(getProperty "MYSQL_DBNAME")

python scripts/update_smfr_nuts_tables.py

./singlenode_down.sh
