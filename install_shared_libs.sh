#!/usr/bin/env bash

LIBS="all models clients utils annotator"
lib=${1:-all}

echo
echo
echo "============== pip install for ${lib}"
echo
echo
if [[ ${lib} == "all" ]]; then
    cp base/shared_libs/VERSION base/shared_libs/smfr_models/
    cp base/shared_libs/VERSION base/shared_libs/smfr_clients/
    cp base/shared_libs/VERSION base/shared_libs/smfr_utils/
    cp base/shared_libs/VERSION base/shared_libs/smfr_annotator/
    pip install base/shared_libs/smfr_models/
    pip install base/shared_libs/smfr_clients/
    pip install base/shared_libs/smfr_utils/
    pip install base/shared_libs/smfr_annotator/
elif [[ -n "`echo ${LIBS} | xargs -n1 echo | grep ${lib}`" ]]; then
    cp base/shared_libs/VERSION base/shared_libs/smfr_${lib}/
    pip install base/shared_libs/smfr_${lib}/
else
    echo Unknown python SMFR package: ${lib}
fi
