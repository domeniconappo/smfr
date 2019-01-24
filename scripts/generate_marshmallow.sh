#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
swagger-marshmallow-codegen --driver=${DIR}/../base/shared_libs/smfr_clients/smfrcore/client/_marshmallow_custom.py:CustomDriver ${DIR}/../restserver/src/swagger/smfr.yaml > ${DIR}/../base/shared_libs/smfr_clients/smfrcore/client/marshmallow.py
