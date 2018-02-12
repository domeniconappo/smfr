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
docker exec -it restserver bash -c "flask db upgrade"
```

## Development guide

# Generate Marshmallow schemas

```bash
$ swagger-marshmallow-codegen swagger/smfr.yaml > server/models/marshmallow.py
```