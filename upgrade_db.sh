#!/usr/bin/env bash

source export_env.sh

./singlenode_dbs.sh
./install_shared_libs.sh models

cd restserver/src

flask db migrate
flask db upgrade

cd -
echo "[WARN] Databases were started. To stop db services execute ./singlenode_down.sh"
