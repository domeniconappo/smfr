#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${DIR}/export_env.sh
mode=${1:-compose}

if [[ ${mode} == "compose" ]]; then
    ${DIR}/singlenode_dbs.sh
elif [[ ${mode} == "swarm" ]]; then
    ${DIR}/swarm_dbs_up.sh
fi

${DIR}/install_shared_libs.sh models

cd ${DIR}/../base/shared_libs/smfr_models/smfrcore/models/sql/migrations

alembic revision --autogenerate
alembic upgrade head

cd -
echo "[WARN] Databases were started. To stop db services execute ./scripts/singlenode_down.sh or ./scripts/swarm_down.sh"
