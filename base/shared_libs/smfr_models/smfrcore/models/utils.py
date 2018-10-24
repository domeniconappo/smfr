import os

import jwt
from flask import jsonify
from werkzeug.exceptions import BadRequest


def get_cassandra_hosts():
    if not bool(int(os.getenv('CASSANDRA_USE_CLUSTER', 0))):
        return [os.getenv('CASSANDRA_HOST', 'cassandrasmfr')]

    return os.getenv('CASSANDRA_NODES', 'cassandrasmfr,cassandra-node-1,cassandra-node-2').split(',')


def error_response(status_code, message):
    resp = {'error': message}
    return jsonify(resp), status_code


def jwt_token(app, payload):
    return jwt.encode(
        payload,
        app.config.get('JWT_SECRET_KEY'),
        algorithm='HS256'
    ).decode('utf-8')


def jwt_decode(app, auth_token):
    return jwt.decode(auth_token, app.config.get('SECRET_KEY'))


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
