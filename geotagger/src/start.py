import logging

from flask import Flask
from flask_restful import Resource, Api

from geotagger import Geotagger


app = Flask(__name__)
api = Api(app)


class GeotaggerApi(Resource):

    logger = logging.getLogger(__name__)

    def put(self, collection_id, action):
        action = action.lower()
        if action not in ('start', 'stop'):
            return {'error': {'description': 'Unknown operation {}'.format(action)}}, 400

        if action == 'start':
            if Geotagger.is_running_for(collection_id):
                return {'error': {'description': 'Geotagger already running for {}'.format(collection_id)}}, 400
            Geotagger.launch_in_background(collection_id)
        elif action == 'stop':
            Geotagger.stop(collection_id)

        return {}, 204


class RunningGeotaggersApi(Resource):

    logger = logging.getLogger(__name__)

    def get(self):
        return Geotagger.running, 200


api.add_resource(GeotaggerApi, '/<int:collection_id>/<string:action>')
api.add_resource(RunningGeotaggersApi, '/running')
