import socket


def running_in_docker():
    with open('/proc/1/cgroup', 'rt') as f:
        return 'docker' in f.read()


def docker_ip():
    print(
        (
                (
                        [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")]
                        or [[(s.connect(("8.8.8.8", 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]
                ) + ["no IP found"]
        )[0]
    )
