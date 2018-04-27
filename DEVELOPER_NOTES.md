# Development notes

## Manage DB migrations

Whenever you add new SQLAlchemy models (or add new fields to existing models), you have to create migrations:

```bash
$ flask db migrate
$ git commit -am"added migrations"
$ git push origin <my_branch>
```

## Generate Marshmallow schemas from smfr.yaml Swagger definitions using a Marshmallow custom driver

```bash
$ cd smfr_core
$ swagger-marshmallow-codegen --driver=./smfrcore/client/_marshmallow_custom.py:CustomDriver ../rest_server/swagger/smfr.yaml > ./smfrcore/client/marshmallow.py
```

## Rebuild and push smfr_base Docker image

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

## Configure a Swarm Cluster

### Init a manager

```bash
$ docker swarm init --advertise-addr 173.212.215.151
Swarm initialized: current node (crl690adjge5ckezkkyg7cnk7) is now a manager.

To add a worker to this swarm, run the following command:

    docker swarm join --token SWMTKN-1-32p2jxigxtkld2cnaj35kl1ra2pczwrbrgqlahje2yj6z1tkuq-4nbm1luuhrd1tseu0qw1bly65 173.212.215.151:2377

To add a manager to this swarm, run 'docker swarm join-token manager' and follow the instructions.
```

Run the output command on other nodes and check the swarm cluster status with `docker info`.
To view the swarm cluster:

```bash
$ docker node ls

ID                            HOSTNAME                      STATUS              AVAILABILITY        MANAGER STATUS
crl690adjge5ckezkkyg7cnk7 *   vmi149143.contaboserver.net   Ready               Active              Leader
lduv1m62pwgbk3925snj8b164     vmi149158.contaboserver.net   Ready               Active
dr0k79jxagxt5kf4mdqmyfhb5     vmi156891.contaboserver.net   Ready               Active
```

Set these ID values in .env convifguration file, where NODE0 is the manager's ID and NODE1 and NODE2 are the workers' IDs:

```ini
NODE0=crl690adjge5ckezkkyg7cnk7
NODE1=lduv1m62pwgbk3925snj8b164
NODE2=dr0k79jxagxt5kf4mdqmyfhb5
```
