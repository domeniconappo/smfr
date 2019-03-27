import os
from decimal import Decimal
import datetime

import numpy as np
from cassandra.util import OrderedMapSerializedKey
from sqlalchemy_utils import Choice
from flask.json import JSONEncoder
from flask import jsonify
from werkzeug.exceptions import BadRequest


def get_cassandra_hosts():
    if not bool(int(os.getenv('CASSANDRA_USE_CLUSTER', 0))):
        return [os.getenv('CASSANDRA_HOST', 'cassandrasmfr')]

    return os.getenv('CASSANDRA_NODES', 'cassandrasmfr,cassandra-node-1,cassandra-node-2').split(',')


def error_response(status_code, message):
    resp = {'error': message}
    return jsonify(resp), status_code


def response(status_code=201, **kwargs):
    return jsonify(**kwargs), status_code


def get_auth_token_from_headers(request):
    # Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJl....
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        return
    tokens = auth_header.split()
    if len(tokens) < 2 or tokens[0] != 'Bearer':
        return
    auth_token = tokens[1]
    return auth_token


def get_json_from_request(request):
    try:
        post_data = request.get_json()
    except BadRequest:
        return None
    else:
        return post_data


class CustomJSONEncoder(JSONEncoder):
    """

    """

    def default(self, obj):
        if isinstance(obj, (np.float32, np.float64, Decimal)):
            return float(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, Choice):
            return float(obj.code)
        elif isinstance(obj, (np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, OrderedMapSerializedKey):
            res = {}
            for k, v in obj.items():
                if isinstance(v, tuple):
                    try:
                        res[k] = dict((v,))
                    except ValueError:
                        res[k] = (v[0], v[1])
                else:
                    res[k] = v

            return res
        return super().default(obj)


smfr_json_encoder = CustomJSONEncoder().default
