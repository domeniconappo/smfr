#!/usr/bin/env bash

LIBS="all models clients utils"
lib=${1:-all}

echo
echo
echo "============== pip install for ${lib}"
echo
echo
if [ ${lib} == "all" ]; then
    pip install base/shared_libs/smfr_models/
    pip install base/shared_libs/smfr_clients/
    pip install base/shared_libs/smfr_utils/
elif [ -n "`echo ${LIBS} | xargs -n1 echo | grep ${lib}`" ]; then
    pip install base/shared_libs/smfr_${lib}/
else
    echo Unknown python SMFR package: ${lib}
fi
