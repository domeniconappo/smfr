import logging
import os

from flask import Flask
from flask_restful import Resource, Api, marshal_with, fields

from aggregator import Aggregator

app = Flask(__name__)
api = Api(app)

LOGGER_FORMAT = '%(asctime)s: Aggregator - <%(name)s>[%(levelname)s] (%(threadName)-10s) %(message)s'
DATE_FORMAT = '%Y%m%d %H:%M:%S'

logging.basicConfig(format=LOGGER_FORMAT, datefmt=DATE_FORMAT)


class AggregatorApi(Resource):
    """
    Flask Restful API for Aggregator microservice (start/stop methods)
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.getLevelName(os.environ.get('LOGGING_LEVEL', 'DEBUG')))

    @marshal_with({'error': fields.Nested({'description': fields.Raw}),
                   'result': fields.Raw,
                   'action_performed': fields.Raw})
    def put(self, collection_id, lang, action):
        action = action.lower()
        if action not in ('start', 'stop'):
            return {'error': {'description': 'Unknown operation {}'.format(action)}}, 400

        if action == 'start':
            Aggregator.launch_in_background(collection_id, lang)
        elif action == 'stop':
            Aggregator.stop(collection_id, lang)

        return {'result': 'success', 'action_performed': action}, 201


api.add_resource(AggregatorApi, '/<int:collection_id>/<string:lang>/<string:action>')
AggregatorApi.logger.info('Aggregator Microservice ready for incoming requests')
Aggregator.log_config()
