# SMFR

## Installation and Configuration

## Docker configuration

- Ensure to have installed Docker and Docker Compose
- Set env variables
  -  MYSQL_DATA_PATH
  -  CASSANDRA_DATA_PATH
  -  ...

## Init and manage Databases

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


**_Note: You have to create migrations on development and push them to GIT._**

In addition to SMFR web interface, you can use the CLI to manage SMFR services:

```bash
docker exec restserver flask list_collections
docker exec restserver flask ...
```


## Development guide

# Generate Marshmallow schemas from smfr.yaml Swagger definitions using a Marshmallow custom driver

```bash
$ cd rest_server/src
$ swagger-marshmallow-codegen --driver=../../client/_marshmallow_custom.py:CustomDriver swagger/smfr.yaml > ../../client/marshmallow.py
```