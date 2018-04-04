from cassandra.util import OrderedMapSerializedKey
import numpy as np

from flask.json import JSONEncoder


def running_in_docker():
    with open('/proc/1/cgroup', 'rt') as f:
        return 'docker' in f.read()


class CustomJSONEncoder(JSONEncoder):

    def default(self, obj):
        if isinstance(obj, (np.float32, np.float64)):
            # obj = 1. * obj
            return float(obj)
        elif isinstance(obj, (np.int32, np.int64)):
            # obj = 1 * obj
            return int(obj)
        elif isinstance(obj, OrderedMapSerializedKey):
            res = {}
            for k, v in obj.items():
                res[k] = (v[0], float(v[1]))
            return res
        return super().default(obj)
