#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${DIR}/export_env.sh
mode=${1:-compose}
action=${2:-generate}

if [[ ${mode} == "compose" ]]; then
    ${DIR}/singlenode_dbs.sh
elif [[ ${mode} == "swarm" ]]; then
    ${DIR}/swarm_dbs_up.sh
fi

${DIR}/install_shared_libs.sh models
echo
echo
echo "================== RUN MIGRATIONS =================="
cd ${DIR}/../base/shared_libs/smfr_models/smfrcore/models/sql/migrations
echo
echo
echo " current revision ------> "
alembic current
if [[ ${action} == "generate" ]]; then
    alembic revision --autogenerate
elif [[ ${action} == "upgrade" ]]; then
    alembic upgrade head
    echo " current revision ------> "
    alembic current
fi
echo "================== END MIGRATIONS =================="

cd -
echo "[WARN] Databases were started. To stop db services execute ./scripts/singlenode_down.sh or ./scripts/swarm_down.sh"
