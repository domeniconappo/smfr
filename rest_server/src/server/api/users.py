from flask import request, abort
from flask_jwt_extended import jwt_required, create_access_token, current_user, get_current_user, get_jwt_identity

from smfrcore.models.sqlmodels import User
from smfrcore.client.marshmallow import User as UserSchema


def signup():
    email = request.json.get('email')
    password = request.json.get('password')
    name = request.json.get('name')
    if email is None or password is None:
        abort(400)  # missing arguments
    if User.query.filter_by(email=email).first() is not None:
        abort(409)  # existing user
    user = User(email=email, name=name)
    user.password_hash = User.hash_password(password)
    user.save()
    return {'email': user.email, 'id': user.id, 'name': name,
            'access_token': create_access_token(identity=email)}, 201


def signin():
    email = request.json.get('email')
    password = request.json.get('password')
    user = User.query.filter_by(email=email).first()
    if not user:
        abort(400)  # non existing user
    if not user.verify_password(password):
        abort(401)  # wrong pass
    users_schema = UserSchema()
    res = users_schema.dump(user).data
    res.update({'access_token': create_access_token(identity=email)})
    return res, 200


@jwt_required
def get_users():
    users = User.query.limit(100).all()
    users_schema = UserSchema()
    res = users_schema.dump(users, many=True).data
    return {'users': res}, 200


@jwt_required
def get_user(email):
    identity = get_jwt_identity()
    if identity != email:
        abort(403)
    user = User.query.filter_by(email=email).first()
    users_schema = UserSchema()
    res = users_schema.dump(user).data
    return res, 200
