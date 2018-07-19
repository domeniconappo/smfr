# import os
#
# from flask import Flask
#
# from smfrcore.utils import RUNNING_IN_DOCKER
# from smfrcore.models.sqlmodels import sqldb
#
#
# def create_app():
#     _mysql_host = '127.0.0.1' if not RUNNING_IN_DOCKER else os.environ.get('MYSQL_HOST', 'mysql')
#     _mysql_db_name = os.environ.get('MYSQL_DBNAME', 'smfr')
#     _mysql_user = os.environ.get('MYSQL_USER')
#     _mysql_pass = os.environ.get('MYSQL_PASSWORD')
#     app = Flask('geocoder Micro Service')
#     app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://{}:{}@{}/{}?charset=utf8mb4'.format(
#         _mysql_user, _mysql_pass, _mysql_host, _mysql_db_name
#     )
#     app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#     sqldb.init_app(app)
#     return app
