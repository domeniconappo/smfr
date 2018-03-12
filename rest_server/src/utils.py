import socket

from flask.json import JSONEncoder
from numpy import float32


def running_in_docker():
    with open('/proc/1/cgroup', 'rt') as f:
        return 'docker' in f.read()


def docker_ip():
    # TODO to remove
    print(
        (
                (
                        [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")]
                        or [[(s.connect(("8.8.8.8", 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]
                ) + ["no IP found"]
        )[0]
    )


class CustomJSONEncoder(JSONEncoder):

    def default(self, obj):
        if isinstance(obj, float32):
            obj = 1. * obj
            return obj
        return super().default(obj)
