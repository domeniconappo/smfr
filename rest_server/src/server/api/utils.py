import os


def is_path(p):
    return (p.startswith('~/') or p.startswith('./') or p.startswith('/') or '/' in p) and (os.path.isfile(p) or os.path.isdir(p))


def normalize_path(p):
    return os.path.normpath(p)


def normalize_payload(payload):
    for k, v in payload.items():
        if is_path(v):
            payload[k] = normalize_path(v)
    return payload
