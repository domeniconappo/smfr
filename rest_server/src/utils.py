def running_in_docker():
    with open('/proc/1/cgroup', 'rt') as f:
        return 'docker' in f.read()
