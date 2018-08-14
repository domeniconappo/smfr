import logging
import os

from smfrcore.utils import LOGGER_FORMAT, LOGGER_DATE_FORMAT

from flask import Flask
from flask_restful import Resource, Api, marshal_with, fields, marshal_with_field

from annotator import Annotator

logging.basicConfig(level=os.environ.get('LOGGING_LEVEL', 'DEBUG'), format=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)
logger = logging.getLogger(__name__)

app = Flask(__name__)
api = Api(app)

logging.getLogger('cassandra').setLevel(logging.WARNING)
logging.getLogger('kafka').setLevel(logging.WARNING)


class AnnotatorApi(Resource):
    """
    Flask Restful API for Annotator microservice (start/stop methods)
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(os.environ.get('LOGGING_LEVEL', 'DEBUG'))

    @marshal_with({'error': fields.Nested({'description': fields.Raw}), 'result': fields.Raw, 'action_performed': fields.Raw})
    def put(self, collection_id, action):
        action = action.lower()
        if action not in ('start', 'stop'):
            return {'error': {'description': 'Unknown operation {}'.format(action)}}, 400

        if action == 'start':
            if Annotator.is_running_for(collection_id):
                return {'error': {'description': 'Annotator already running (collection: {})'.format(collection_id)}}, 400
            Annotator.launch_in_background(collection_id)
        elif action == 'stop':
            Annotator.stop(collection_id)

        return {'result': 'success', 'action_performed': action}, 201


class RunningAnnotatorsApi(Resource):
    """
    API for `/running` endpoint
    """

    @marshal_with_field(fields.List(fields.List(fields.Raw)))
    def get(self):
        return Annotator.running(), 200


class AnnotatorModels(Resource):
    """
    API for `/models` endpoint
    """

    @marshal_with_field(fields.Raw)
    def get(self):
        return Annotator.available_models(), 200


if __name__ == 'start':

    api.add_resource(AnnotatorApi, '/<int:collection_id>/<string:action>')
    api.add_resource(RunningAnnotatorsApi, '/running')
    api.add_resource(AnnotatorModels, '/models')
    AnnotatorApi.logger.info('[OK] Annotator Microservice ready for incoming requests')

    Annotator.log_config()

    # start topic consumers for pipeline
    development = bool(int(os.environ.get('DEVELOPMENT', 0)))
    for language in Annotator.available_languages:
        # if running docker compose on a single server/development,
        # we just bootstrap Annotator EN to avoid eating the whole memory
        if development and language == 'en' or not development:
            Annotator.logger.info('----- Starting KAFKA consumer on topic: annotator_%s', language)
            Annotator.consumer_in_background(language)
