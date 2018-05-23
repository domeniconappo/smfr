from functools import wraps

from flask import abort
from flask_jwt_extended import get_jwt_identity

from smfrcore.models.sqlmodels import User


# TODO MAKE IT WORKING..NOW IT'S FAKE
def check_identity(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        # identity = get_jwt_identity()
        # user = User.query.filter_by(email=identity).first()
        # if user.email != identity:
        #     abort(403)
        return fn(*args, **kwargs)
    return wrapper


# TODO MAKE IT WORKING..NOW IT'S FAKE
def check_role(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        # identity = get_jwt_identity()
        # user = User.query.filter_by(email=identity).first()
        # if user.email != identity:
        #     abort(403)
        return fn(*args, **kwargs)
    return wrapper
