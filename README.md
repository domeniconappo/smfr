# SMFR

## Installation and Configuration

### Docker configuration

- Ensure to have installed Docker and Docker Compose
- Copy _.env.tpl_ file to _.env_ and edit the last one: `cp .env.tpl .env`
- Execute `./build.sh` (use `sudo` in case you need it to execute docker-compose commands)
- Execute `docker-compose up -d` or `sudo docker-compose up -d`

- Connect to interface by pointing your browser to http://localhost:8888
- REST Server API responds to http://localhost:5555/1.0 calls.
- Swagger UI is available at http://localhost:5555/1.0/ui
- Elasticsearch at http://localhost:9200

### Init and manage Databases

#### Geonames

##### Check that Geonames index is up in ES:
Connect to http://localhost:9200/_cat/indices?v
You should see something like (check __docs.count__ and __store.size__):

```
health status index    uuid                   pri rep docs.count docs.deleted store.size pri.store.size
yellow open   geonames 23vFz20STbudmqktmHVOLg   1   1   11139265            0      2.7gb          2.7gb
```

Troubleshooting for Elasticsearch

```
The vm_map_max_count setting should be set permanently in /etc/sysctl.conf:

$ grep vm.max_map_count /etc/sysctl.conf
vm.max_map_count=262144
```

Usage of mordecai library from restserver image:

```python

In [1]: import mordecai
/DATA/virtualenvs/smfr/lib/python3.6/site-packages/h5py/__init__.py:36: FutureWarning: Conversion of the second argument of issubdtype from `float` to `np.floating` is deprecated. In future, it will be treated as `np.float64 == np.dtype(float).type`.
  from ._conv import register_converters as _register_converters
Using TensorFlow backend.

In [2]: g=mordecai.Geoparser

In [3]: g=mordecai.Geoparser()
2018-04-04 16:15:39.099405: I tensorflow/core/platform/cpu_feature_guard.cc:140] Your CPU supports instructions that this TensorFlow binary was not compiled to use: AVX2

In [4]: g.geoparse('Going from Rome to New York')
Out[4]:
[{'country_conf': 0.97380584,
  'country_predicted': 'ITA',
  'geo': {'admin1': 'Latium',
   'country_code3': 'ITA',
   'feature_class': 'P',
   'feature_code': 'PPLC',
   'geonameid': '3169070',
   'lat': '41.89193',
   'lon': '12.51133',
   'place_name': 'Rome'},
  'spans': [{'end': 4, 'start': 0}],
  'word': 'Rome'},
 {'country_conf': 0.9998105,
  'country_predicted': 'USA',
  'geo': {'admin1': 'New York',
   'country_code3': 'USA',
   'feature_class': 'P',
   'feature_code': 'PPL',
   'geonameid': '5128581',
   'lat': '40.71427',
   'lon': '-74.00597',
   'place_name': 'New York City'},
  'spans': [{'end': 8, 'start': 0}],
  'word': 'New York'}]

```


#### MySQL

Whenever new models (or new fields) are added to the mysql schema, follow these steps to update DB:
On development:

```
flask db migrate
flask db upgrade
```

On dockerized server, once the migrations are under SCM:

```bash
docker exec restserver flask db upgrade
```

If smfr_restserver image has problems to start due "unsynced" db tables, try the following command

```bash
docker run -e FLASK_APP=smfr.py --entrypoint='flask' smfr_restserver 'db upgrade'
```

From host, connect to MySQL DB as the docker root user with `mysql -h 127.0.0.1 -p` (if you have mysql client installed) or just use docker exec:
`docker exec -it mysql mysql -h 127.0.0.1 -p`

**_Note: You have to create migrations in development and push them to GIT repo. Then you have to apply migrations on all systems where SMFR runs (dev, test, prod etc.)_**

**_Note: If MySQL operations are extremely slow, this can depend on filesystem settings on the Linux OS. Follow this article to fix: http://phpforus.com/how-to-make-mysql-run-fast-with-ext4-on-ubuntu/_**



#### Cassandra

Table migrations (i.e. new columns) will be automatically added by CQLAlchemy.
**_Note: New columns are added by CQLAlchemy but you have to manually drop or alter types of existing columns using cqlsh._**

From host, use cqlsh on docker container to connect to DB: `docker exec -it cassandra cqlsh`


## Interfaces

Connect to http://localhost:8888/ for SMFR web interface.

In addition to SMFR web interface, you can use the CLI to manage SMFR services:

```bash
docker exec restserver flask list_collections
docker exec restserver flask ...
```


## Development guide

### Generate Marshmallow schemas from smfr.yaml Swagger definitions using a Marshmallow custom driver

```bash
$ cd rest_server/src
$ swagger-marshmallow-codegen --driver=../../client/_marshmallow_custom.py:CustomDriver swagger/smfr.yaml > ../../client/marshmallow.py
```

### Free some disk space from unused 'dockers'
```
docker images --no-trunc | grep '<none>' | awk '{ print $3 }' | xargs -r docker rmi
```

```
docker-compose down --rmi all --remove-orphans
```

Use the following script to clean up everything at once: https://lebkowski.name/docker-volumes/

```bash
#!/bin/bash

# remove exited containers:
docker ps --filter status=dead --filter status=exited -aq | xargs -r docker rm -v

# remove unused images:
docker images --no-trunc | grep '<none>' | awk '{ print $3 }' | xargs -r docker rmi

# remove unused volumes:
find '/var/lib/docker/volumes/' -mindepth 1 -maxdepth 1 -type d | grep -vFf <(
  docker ps -aq | xargs docker inspect | jq -r '.[] | .Mounts | .[] | .Name | select(.)'
) | xargs -r rm -fr
```
