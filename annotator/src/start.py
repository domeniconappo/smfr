import logging
import os

from flask import Flask
from flask_restful import Resource, Api, marshal_with, fields, marshal_with_field

from annotator import Annotator


app = Flask(__name__)
api = Api(app)


class AnnotatorApi(Resource):
    """
    Flask Restful API for Annotator microservice (start/stop methods)
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(os.environ.get('LOGGING_LEVEL', 'DEBUG'))

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


if __name__ == '__main__':
    singleserver = bool(int(os.environ.get('SINGLENODE', 0)))
    update_models = bool(int(os.environ.get('UPDATE_ANNOTATOR_MODELS', 0)))
    
    api.add_resource(AnnotatorApi, '/<int:collection_id>/<string:lang>/<string:action>')
    api.add_resource(RunningAnnotatorsApi, '/running')
    api.add_resource(AnnotatorModels, '/models')
    if update_models:
        Annotator.logger.info('GIT: Check if there are new models on repository...')
        Annotator.download_cnn_models()

    AnnotatorApi.logger.info('[OK] Annotator Microservice ready for incoming requests')

    Annotator.log_config()

    # start topic consumers for pipeline
    for language in Annotator.available_languages:
        # if running docker compose on a single server, we just bootstrap Annotator EN to avoid out of memory errors
        if singleserver and language == 'en' or not singleserver:
            Annotator.logger.info('----- Starting KAFKA consumer on topic: annotator_%s', language)
            Annotator.consumer_in_background(language)
