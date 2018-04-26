# SMFR

## Installation and Configuration

### Docker configuration

- Ensure to have installed Docker and Docker Compose
- Get the source code: `git clone https://github.com/domeniconappo/SMFR.git`
- Enter into SMFR folder and copy _.env.tpl_ file to _.env_.
  - `$ cp .env.tpl .env`
- Edit the last one: `$ cp .env.tpl .env` by setting the following variables
    - `SMFR_DATADIR=/DATA/smfr/data`
      -   The server folder where MySQL, Cassandra and Elasticsearch data folders will be mapped to.
    - `CASSANDRA_KEYSPACE=smfr_persistent`
      -   Cassandra keyspace name
    - `KAFKA_TOPIC=persister`
      - The KAFKA topic name
    - `MIN_FLOOD_PROBABILITY=0.59`
      - minimum flood probability for which the text is considered "positive"
    - `LOGGING_LEVEL=DEBUG`
      - Logging level. In production this should be WARNING or ERROR
    - `GIT_REPO_MODELS`=https://user:pass@bitbucket.org/lorinivalerio/smfr_models_data.git
      - You have to include Bitbucket credentials of a user with read permissions to the SMFR models repository
    - `DOCKER_ID_USER`=efas
      - This is the Docker user that must login before to build and push the base_smfr Docker image. This must be set only for project contributors of base SMFR image.
- Execute `./build.sh` (use `sudo` in case you need it to execute docker-compose commands). This step can take several minutes.
- Execute `docker-compose up -d` or `sudo docker-compose up -d`
- You will see all services starting:
    - cassandra
    - mysql
    - geonames
    - annotator
    - geocoder
    - restserver
    - web
- Wait a couple of minutes for services to get "warm" and connect to each other.
- Connect to the web interface by pointing your browser to http://localhost:8888
- REST Server API responds to http://localhost:5555/1.0 calls.
- Swagger UI is available at http://localhost:5555/1.0/ui
- Elasticsearch geonames instance at http://localhost:9200

### Init and manage Databases

#### Geonames

Check that Geonames index is up in ES:
Connect to http://localhost:9200/_cat/indices?v
You should see something like (check __docs.count__ and __store.size__):

```
health status index    uuid                   pri rep docs.count docs.deleted store.size pri.store.size
yellow open   geonames 23vFz20STbudmqktmHVOLg   1   1   11139265            0      2.7gb          2.7gb
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


### Troubleshooting

This sections tries to address all kind of issues you can have at system level when you run SMFR suite.
Useful commands for troubleshooting:

```
docker-compose logs restserver web geonames
```

#### Troubleshooting for Geonames Elasticsearch
If you see an error/warning in Elasticsearch logs, the vm_map_max_count setting should be set permanently in /etc/sysctl.conf:

```


$ grep vm.max_map_count /etc/sysctl.conf
vm.max_map_count=262144
```

#### Free some disk space from unused 'dockers'

```bash
docker images --no-trunc | grep '<none>' | awk '{ print $3 }' | xargs -r docker rmi
```

```bash
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


## Interfaces

Connect to http://localhost:8888/ for SMFR web interface.

In addition to SMFR web interface, you can use the CLI to manage SMFR services:

```bash
$ docker exec restserver flask  # to see list of available commands
$ docker exec restserver flask list_collections
$ docker exec restserver flask ...
```


## Development notes

### Manage DB migrations

Whenever you add new SQLAlchemy models (or add new fields to existing models), you have to create migrations:

```bash
$ flask db migrate
$ git commit -am"added migrations"
$ git push origin <my_branch>
```

### Generate Marshmallow schemas from smfr.yaml Swagger definitions using a Marshmallow custom driver

```bash
$ cd smfr_core
$ swagger-marshmallow-codegen --driver=./smfrcore/client/_marshmallow_custom.py:CustomDriver ../rest_server/swagger/smfr.yaml > ./smfrcore/client/marshmallow.py
```

### Rebuild and push smfr_base Docker image

Whenever any change is made on smfr_base docker image (e.g. modifications to smfrcore.models
code), the image must be rebuilt and pushed on docker registry.
To push the image on efas Docker space, first you need to login with Docker:

```bash
$ export DOCKER_ID_USER="ask_for_efas_docker_username"
$ docker login
```

Then, you can push the image.
```bash
$ docker build --build-arg http_proxy=${http_proxy} --build-arg https_proxy=${http_proxy} -t smfr_base base_docker/.
$ docker tag smfr_base efas/smfr_base
$ docker push efas/smfr_base
```

or just launch the build shell script to build everything.

```bash
$ ./build.sh
```