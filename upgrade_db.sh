#!/usr/bin/env bash

source functions.sh

./singlenode_up.sh mysql
./install_shared_libs.sh models

export FLASK_APP=smfr.py
export MYSQL_USER=$(getProperty "MYSQL_USER")
export MYSQL_PASSWORD=$(getProperty "MYSQL_PASSWORD")
export MYSQL_DBNAME=$(getProperty "MYSQL_DBNAME")
cd rest_server/src

flask db migrate
flask db upgrade

cd -
./singlenode_down.sh
