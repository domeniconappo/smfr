#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
LIBS="all models clients utils analysis"
lib=${1:-all}

echo
echo
echo "============== pip install for ${lib}"
echo
echo
if [[ ${lib} == "all" ]]; then
    cp ${DIR}/../VERSION ${DIR}/../base/shared_libs/smfr_models/
    cp ${DIR}/../VERSION ${DIR}/../base/shared_libs/smfr_clients/
    cp ${DIR}/../VERSION ${DIR}/../base/shared_libs/smfr_utils/
    cp ${DIR}/../VERSION ${DIR}/../base/shared_libs/smfr_analysis/
    pip install ${DIR}/../base/shared_libs/smfr_models/
    pip install ${DIR}/../base/shared_libs/smfr_clients/
    pip install ${DIR}/../base/shared_libs/smfr_utils/
    pip install ${DIR}/../base/shared_libs/smfr_analysis/
elif [[ -n "`echo ${LIBS} | xargs -n1 echo | grep ${lib}`" ]]; then
    cp ${DIR}/../VERSION ${DIR}/../base/shared_libs/smfr_${lib}/
    pip install ${DIR}/../base/shared_libs/smfr_${lib}/
else
    echo Unknown python SMFR package: ${lib}
fi
