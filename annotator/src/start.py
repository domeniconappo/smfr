import logging
import os

from flask import Flask
from flask_restful import Resource, Api, marshal_with, fields, marshal_with_field

from annotator import Annotator

app = Flask(__name__)
api = Api(app)

LOGGER_FORMAT = '%(asctime)s: Annotator - <%(name)s>[%(levelname)s] (%(threadName)-10s) %(message)s'
DATE_FORMAT = '%Y%m%d %H:%M:%S'

logging.basicConfig(format=LOGGER_FORMAT, datefmt=DATE_FORMAT)


class AnnotatorApi(Resource):
    """
    Flask Restful API for Annotator microservice (start/stop methods)
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.getLevelName(os.environ.get('LOGGING_LEVEL', 'DEBUG')))

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
    Flask Restful API for Annotator microservice for `/running` endpoint
    """

    logger = logging.getLogger(__name__)

    @marshal_with_field(fields.List(fields.List(fields.Raw)))
    def get(self):
        return Annotator.running(), 200


api.add_resource(AnnotatorApi, '/<int:collection_id>/<string:lang>/<string:action>')
api.add_resource(RunningAnnotatorsApi, '/running')
Annotator.download_cnn_models()
AnnotatorApi.logger.info('Annotator Microservice ready for incoming requests')
Annotator.log_config()
