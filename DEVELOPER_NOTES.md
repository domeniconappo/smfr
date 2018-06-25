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

To deploy:

```bash
$  ./deploy.sh
```

### SWARM Troubleshooting

Need to open ports on all hosts participating in a Swarm cluster.
Also, Cassandra ports must be open as well.

On manager node

```bash
sudo iptables -A INPUT -p tcp --dport 22 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 2376 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 2377 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 7946 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 7946 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 4789 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9042 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9160 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 7000 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 7001 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 7199 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 9042 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 9160 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 7000 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 7001 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 7199 -j ACCEPT
sudo netfilter-persistent save
sudo systemctl restart docker
```

On all worker nodes

```bash
sudo iptables -A INPUT -p tcp --dport 22 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 2376 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 7946 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 7946 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 4789 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 9042 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 9160 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 7000 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 7001 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 7199 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9042 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9160 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 7000 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 7001 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 7199 -j ACCEPT
sudo netfilter-persistent save
sudo systemctl restart docker
```

# Docker upgrade

Resolve cgroup issue with

```bash
sudo mkdir /sys/fs/cgroup/systemd
sudo mount -t cgroup -o none,name=systemd cgroup /sys/fs/cgroup/systemd
```


# Internal Docker Swarm cluster EFAS Groups


ID                            HOSTNAME            STATUS              AVAILABILITY        MANAGER STATUS      ENGINE VERSION
5xvoosrucfqumbzzzie3incea     D01RI1000318        Ready               Active                                  18.03.1-ce
3jvsn4my7acqzjhvb09nfa6l0     D01RI1303461        Ready               Active                                  18.03.1-ce
to5yzzql1ok0dr6x8r9lfn1rm *   D01RI1303463        Ready               Active              Leader              18.03.1-ce
