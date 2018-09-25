# SMFR

## Introduction

Social Media Flood Risk (SMFR) is a platform to monitor specific flood events
on social media (currently, only Twitter).

The software is developed as Exploratory Research project at the JRC - European Commission facilities and is released under [EUPL licence](LICENCE.md).

Current version is based on:
  - Twitter Stream API to filter tweets using keywords and/or bounding box criteria
  - ML algorithms and models for classification of tweets (flood relevance)
  - Geonames index for geocoding of relevant tweets
  - Cassandra to keep raw tweets data
  - Kafka and MySQL as infrastructure elements

When a potential catastrofic flood event is recorded in EFAS, SMFR is notified and will start to:

  - Collect tweets with Twitter Stream API
  - An operator will access to the web interface to manually start annotation.
  - When annotation is completed, an operator will manually start geocoding.

Final product of SMFR is a event-related map reporting relevant tweets.


## Installation and Configuration

### Docker configuration

- Ensure to have installed Docker and Docker Compose
- Get the source code: `$ git clone https://github.com/domeniconappo/SMFR.git`
- Enter into SMFR folder and copy _.env.tpl_ file to _.env_.
  - `$ cp .env.tpl .env`
- Edit the file `.env` and change defaults according your needs:
    - `SMFR_DATADIR=/DATA/smfr/data`
      -   The server folder where MySQL and Cassandra data folders will be mapped to.
    - `CASSANDRA_KEYSPACE=smfr_persistent`
      -   Cassandra keyspace name
    - `PERSISTER_KAFKA_TOPIC=persister`
      - The KAFKA topic name
    - `MIN_FLOOD_PROBABILITY=0.59`
      - minimum flood probability for which the text is considered "positive"
    - `LOGGING_LEVEL=DEBUG`
      - Logging level. In production this should be WARNING or ERROR
    - `GIT_REPO_MODELS=https://user:pass@bitbucket.org/lorinivalerio/smfr_models_data.git`
      - You have to include Bitbucket credentials of a user with read permissions to the SMFR models repository
    - `DOCKER_ID_USER=efas`
      - Docker swarm node ids to set up for swarm deploy. Check [Developer Notes](DEVELOPER_NOTES.md) for more info.
- Copy all yaml.tpl files to have extension yaml and edit them according your configuration
  (e.g. admin_collector.yaml which contains Twitter client keys and secrets)
- Execute `./build.sh` if you need to rebuild images. This step can take several minutes and will also push updates to Docker registry in case the DOCKER_ID_USER is set and it's got rights to push.
    - __Note__: It's not possible to build images with `docker-compose build` command
     as there are some variables substitution to perform (e.g. current branch name giving the image tag).
In this case, you must login to Docker Hub (just issue `$ docker login` before to build).
- Execute `$ ./singlenode_up.sh` for local testing or `$ ./swarm_up.sh` script if you deploy to a Docker Swarm cluster

- You will see all services starting:
    - cassandrasmfr
    - mysql
    - phpmyadmin
    - kafka
    - geonames (Gazzetter)
    - persister
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

#### Cassandra

Create a new SUPERUSER in cassandra (with same user and password you set up in your jmxremote.access)

```bash
$ docker exec -it cassandrasmfr cqlsh -u cassandra -p cassandra
Connected to Test Cluster at 127.0.0.1:9042.
cassandra@cqlsh> CREATE USER myuser WITH PASSWORD 'mypassword' SUPERUSER;
cassandra@cqlsh> ALTER USER cassandra WITH PASSWORD 'anotherpassword';
```

#### Geonames

Check that Geonames index is up in ES:
Connect to http://localhost:9200/_cat/indices?v
You should see something like the output below (check __docs.count__ and __store.size__):

```
health status index    uuid                   pri rep docs.count docs.deleted store.size pri.store.size
yellow open   geonames 23vFz20STbudmqktmHVOLg   1   1   11139265            0      2.7gb          2.7gb
```


#### MySQL

Whenever new models (or new fields) are added to the mysql schema, execute the following script to update SMFR DB.

```bash
$ ./upgrade_db.sh
```

The script will:

- start MySQL container only
- install models package in a virtualenv
- execute flask alembic commands
- shutdown MySQL container

The last command `flask db upgrade` that performs the actual schema migration,
it's executed at each startup of the rest server image anyway so upgrade_db.sh is used in DEV phase only, to add migrations files.

**_Note: You have to create migrations in development and push them to GIT repo.
Then you have to apply migrations on all systems where SMFR runs (dev, test, prod etc.)_**

##### Upgrading Nuts tables

When a new version of nuts tables is available (compressed json files are under scripts/seeds/data/ repository folder), execute:

```bash
$ ./upgrade_nuts_tables.sh
```


**_Tip: From host, connect to MySQL DB by using docker exec:_**
`docker exec -it mysql mysql -h 127.0.0.1 -p`


#### Cassandra

Table migrations (i.e. new columns) will be automatically added by CQLAlchemy.
**_Note: New columns are added by CQLAlchemy but you have to manually drop or alter types of existing columns using cqlsh._**

From host, use cqlsh on docker container to connect to DB:
`docker exec -it cassandrasmfr cqlsh -u $CASSANDRA_USER -p $CASSANDRA_PASSWORD`


### Troubleshooting

This sections tries to address all kind of issues you can have at system level when you run SMFR suite.

#### MySQL is slow

MySQL seems to have some problems with ext4 filesystem.
If you are in Ubuntu and MySQL operations are extremely slow, this can depend on filesystem settings.
Follow this article to fix: http://phpforus.com/how-to-make-mysql-run-fast-with-ext4-on-ubuntu/_**


#### Issues with Geonames Elasticsearch and/or Cassandra
If you see an error/warning about vm.max_map_count variable in Elasticsearch logs of Geonames docker image, or in Cassandra docker image, the `vm.map_max_count` setting should be set permanently in /etc/sysctl.conf of the host machine:

```bash
$ grep vm.max_map_count /etc/sysctl.conf
vm.max_map_count=1048575
```

#### Cassandra tombstones
Null values produce cell tombstones. Cassandray will abort queries if tombstones threshold is reached.
To avoid it: set gc_grace_seconds to 0 for tweet table:
```
alter table smfr_persistent.tweet with gc_grace_seconds = 0;
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
