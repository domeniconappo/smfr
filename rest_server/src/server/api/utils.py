import os


def is_path(p):
    return isinstance(p, str) and (p.startswith('~/') or p.startswith('./') or p.startswith('/') or '/' in p)\
           and (os.path.isfile(p) or os.path.isdir(p))


def normalize_payload(payload):
    for k, v in payload.items():
        if is_path(v):
            payload[k] = os.path.normpath(v)
    return payload


class Event:
    @classmethod
    def preview(cls, efas_json_path):
        res = {'event_1': {'location': 'Ispra', 'data': 0.001}}
        return res
