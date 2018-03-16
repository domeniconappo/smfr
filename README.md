# SMFR

## Installation and Configuration

### Docker configuration

- Ensure to have installed Docker and Docker Compose
- Set env variables
  -  MYSQL_DATA_PATH
  -  CASSANDRA_DATA_PATH
  -  ...

### Init and manage Databases

#### MySQL

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

From host, connect to MySQL DB with `mysql -h 127.0.0.1 -P 3306 -u root -p` (if you have mysql client installed) or just use docker exec:
`docker exec -it mysql mysql -h 127.0.0.1 -P 3306 -u root -p`

**_Note: You have to create migrations in development and push them to GIT repo. Then you have to apply migrations on all systems where SMFR runs (dev, test, prod etc.)_**

**_Note: If MySQL operations are extremely slow, this can depend on filesystem settings on the Linux OS. Follow this article to fix: http://phpforus.com/how-to-make-mysql-run-fast-with-ext4-on-ubuntu/_**



#### Cassandra

Table migrations (i.e. new columns) will be automatically added by CQLAlchemy.
**_Note: New columns are added by CQLAlchemy but you have to manually drop or alter types of existing columns using cqlsh._**

From host, use cqlsh on docker container to connect to DB: `docker exec -it cassandra cqlsh`


## Interfaces


In addition to SMFR web interface, you can use the CLI to manage SMFR services:

```bash
docker exec restserver flask list_collections
docker exec restserver flask ...
```


- Connect to interface by pointing your browser to http://localhost:8888
- REST Server API responds to http://localhost:5555/1.0 calls.
- Swagger UI is available at http://localhost:5555/1.0/ui


## Development guide

# Generate Marshmallow schemas from smfr.yaml Swagger definitions using a Marshmallow custom driver

```bash
$ cd rest_server/src
$ swagger-marshmallow-codegen --driver=../../client/_marshmallow_custom.py:CustomDriver swagger/smfr.yaml > ../../client/marshmallow.py
```