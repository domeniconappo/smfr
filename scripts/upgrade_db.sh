#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${DIR}/export_env.sh

${DIR}/singlenode_dbs.sh
${DIR}/install_shared_libs.sh models

cd ${DIR}/../restserver/src

flask db migrate
flask db upgrade

cd -
echo "[WARN] Databases were started. To stop db services execute ./singlenode_down.sh"
