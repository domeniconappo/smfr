## Requirements

To properly run SMFR you need some powerful hardware and lots of disk space (1 TB at least), and a good network link, of course.

The entire solution is Docker Compose based so a running Docker daemon and docker-compose software are required on the machine(s) where you will launch the system. 

In case you opt for the Swarm, the Docker Swarm mode is also required, meaning that you have to configure the Swarm cluster (choosing a manager, adding nodes and labels etc.)

If you opt to use a single machine to run SMFR, ensure the machine meets the _minimum_ requirements enlisted here:

- 8 cores 
- 32 GB Mem
- 500 GB of dedicated disk space
- Access to public docker registry or having an internal docker repository.

## Get the code

SMFR project is open source and it can be freely forked, cloned or downloaded.
To start to use it, easist way is to get the project by cloning the repository:

```bash
git clone https://github.com/ec-jrc/smfr.git
cd smfr
```

## Install requirements in a virtual environment

Even if SMFR services are all containerized, it can be useful to set up a python virtual environment in order to have all requirements and SMFR libraries installed, 
so that you can open a python shell and connect to services for debugging/testing/experimenting purpose.

In order to do that, execute following commands and scripts from the project folder `./smfr/`:

```bash
sudo apt-get install python3-pip
sudo pip3 install virtualenv
virtualenv /home/myuser/venvs/smfr
source /home/myuser/venvs/smfr/bin/activate
./scripts/install_shared_libs.sh
```
