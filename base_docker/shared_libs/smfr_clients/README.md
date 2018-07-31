# SMFR Core Client Module

The folder contains the smfrcore client modules, which will be installed
as a standard python package in containers that needs SMFR core models and tools.

How to create new marshamallow models (whenever you add new definitions models in swagger configuration):


```bash
$ cd shared_libs/smfr_clients
$ swagger-marshmallow-codegen --driver=./smfrcore/client/_marshmallow_custom.py:CustomDriver ../../rest_server/src/swagger/smfr.yaml > ./smfrcore/client/marshmallow.py
```

