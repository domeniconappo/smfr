import os
import logging

from flask import Flask
from flask_restful import Resource, Api, fields, marshal_with, marshal_with_field

from geotagger import Geotagger


app = Flask(__name__)
api = Api(app)

LOGGER_FORMAT = '%(asctime)s: Microservice Geotagger - <%(name)s>[%(levelname)s] (%(threadName)-10s) %(message)s'
DATE_FORMAT = '%Y%m%d %H:%M:%S'

logging.basicConfig(format=LOGGER_FORMAT, datefmt=DATE_FORMAT)


class GeotaggerApi(Resource):

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.getLevelName(os.environ.get('LOGGING_LEVEL', 'DEBUG')))

    @marshal_with({'error': fields.Nested({'description': fields.Raw}), 'result': fields.Raw, 'action_performed': fields.Raw})
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

        return {'result': 'success', 'action_performed': action}, 201


class RunningGeotaggersApi(Resource):

    logger = logging.getLogger(__name__)

    @marshal_with_field(fields.List(fields.Integer))
    def get(self):
        self.logger.info(Geotagger.running)
        return Geotagger.running, 200


api.add_resource(GeotaggerApi, '/<int:collection_id>/<string:action>')
api.add_resource(RunningGeotaggersApi, '/running')
