from flask import Flask
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)


class AnnotatorApi(Resource):

    def put(self, collection_id, action):
        return {'hello': 'world'}


api.add_resource(AnnotatorApi, '/<int:collection_id>/<string:action>')
