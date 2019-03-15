## Requirements

To properly run SMFR you need some powerful hardware and lots of disk space (1 TB at least), and a good network link, of course.

The entire solution is Docker Compose based so a running Docker daemon and docker-compose software are required on the machine(s) where you will launch the system. 

In case you opt for the Swarm, the Docker Swarm mode is also required, meaning that you have to configure the Swarm cluster (choosing a manager, adding nodes and labels etc.)

If you opt to use a single machine to run SMFR, ensure the machine meets the _minimum_ requirements enlisted here:

- 8 cores 
- 32 GB Mem
- 500 GB of dedicated disk space
