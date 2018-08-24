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
$ cd shared_libs/smfr_clients
$ swagger-marshmallow-codegen --driver=./smfrcore/client/_marshmallow_custom.py:CustomDriver ../../rest_server/src/swagger/smfr.yaml > ./smfrcore/client/marshmallow.py
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
$ docker build --build-arg http_proxy=${http_proxy} --build-arg https_proxy=${http_proxy} -t smfr_base base/.
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
ID                            HOSTNAME            STATUS              AVAILABILITY        MANAGER STATUS      ENGINE VERSION
5xvoosrucfqumbzzzie3incea     D01RI1000318        Ready               Active                                  18.03.1-ce
3jvsn4my7acqzjhvb09nfa6l0     D01RI1303461        Ready               Active                                  18.03.1-ce
to5yzzql1ok0dr6x8r9lfn1rm *   D01RI1303463        Ready               Active              Leader              18.03.1-ce
```

Then, add labels `huge`, `large`, `medium` and `small` , based on machines hardware to honour deployment constraints.
Execute commands from a manager node like the one below:

```bash
$ docker node update --label-add large=true D01RI1303463
```



### To deploy/undeploy:

```bash
$ ./swarm_up.sh
$ ./swarm_down.sh
```


### If you lost tokens...

```
$ docker swarm join-token manager

To add a manager to this swarm, run the following command:

docker swarm join --token SWMTKN-1-5r7ft1jj59hvrulvo6fnxo6o42u3qetyoli40lw12e2gxxguui-5f8vwg1b49hgfl2b376v7jni4 139.191.16.193:2377
```

```
$ docker swarm join-token worker
To add a worker to this swarm, run the following command:

 docker swarm join --token SWMTKN-1-5r7ft1jj59hvrulvo6fnxo6o42u3qetyoli40lw12e2gxxguui-8ljuguk8lx0v2vzie0ya0syw1 139.191.16.193:2377
```



# Docker upgrade

Resolve cgroup issue with

```bash
sudo mkdir /sys/fs/cgroup/systemd
sudo mount -t cgroup -o none,name=systemd cgroup /sys/fs/cgroup/systemd
```


# Internal Docker Swarm cluster EFAS Groups


ID                            HOSTNAME            STATUS              AVAILABILITY        MANAGER STATUS      ENGINE VERSION
qs3rsmr6fw5wnhv0prejj4kku     D01RI1000318        Ready               Active                                  18.03.1-ce
cs66xxcbv7y1oof539sxmyif5     D01RI1303461        Ready               Active              Reachable           18.03.1-ce
obii6o2zg744viqdw7mnb3s6a *   D01RI1303463        Ready               Active              Leader              18.03.1-ce
th5ywa0t7z15990eufgxfr9tg     srv-floodsrisk      Ready               Active              Reachable           18.03.1-ce