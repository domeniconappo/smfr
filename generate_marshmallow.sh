#!/usr/bin/env bash
swagger-marshmallow-codegen --driver=./base/shared_libs/smfr_clients/smfrcore/client/_marshmallow_custom.py:CustomDriver ./rest_server/src/swagger/smfr.yaml > ./base/shared_libs/smfr_clients/smfrcore/client/marshmallow.py
