import logging
import os

from flask import Flask
from flask_restful import Resource, Api, marshal_with, fields, marshal_with_field
from smfrcore.utils import LOGGER_FORMAT, LOGGER_DATE_FORMAT

from annotator import Annotator

app = Flask(__name__)
api = Api(app)

logging.basicConfig(level=os.environ.get('LOGGING_LEVEL', 'DEBUG'), format=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)
logger = logging.getLogger(__name__)


class AnnotatorApi(Resource):
    """
    Flask Restful API for Annotator microservice (start/stop methods)
    """

    @marshal_with({'error': fields.Nested({'description': fields.Raw}), 'result': fields.Raw, 'action_performed': fields.Raw})
    def put(self, collection_id, lang, action):
        action = action.lower()
        if action not in ('start', 'stop'):
            return {'error': {'description': 'Unknown operation {}'.format(action)}}, 400
        if lang not in Annotator.models:
            return {'error': {'description': 'No models for language {}'.format(lang)}}, 400

        if action == 'start':
            if Annotator.is_running_for(collection_id, lang):
                return {'error': {'description': 'Annotator already running {}-{}'.format(collection_id, lang)}}, 400
            Annotator.launch_in_background(collection_id, lang)
        elif action == 'stop':
            Annotator.stop(collection_id, lang)

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


api.add_resource(AnnotatorApi, '/<int:collection_id>/<string:lang>/<string:action>')
api.add_resource(RunningAnnotatorsApi, '/running')
api.add_resource(AnnotatorModels, '/models')
logger.info('Check if there are new models...')
Annotator.download_cnn_models()
logger.info('Annotator Microservice ready for incoming requests')
Annotator.log_config()
