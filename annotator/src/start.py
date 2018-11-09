import logging

from flask import Flask
from flask_restful import Resource, Api, marshal_with, fields, marshal_with_field

from annotatorcontainer import AnnotatorContainer, DEVELOPMENT
# from helpers import models, models_path, logger
from smfrcore.ml.helpers import models, models_path, logger, available_languages

app = Flask(__name__)
api = Api(app)

logging.getLogger('cassandra').setLevel(logging.WARNING)
logging.getLogger('kafka').setLevel(logging.WARNING)


class AnnotatorApi(Resource):
    """
    Flask Restful API for Annotator microservice (start/stop methods)
    """

    @marshal_with({'error': fields.Nested({'description': fields.Raw}), 'result': fields.Raw, 'action_performed': fields.Raw})
    def put(self, collection_id, action):
        action = action.lower()
        if action not in ('start', 'stop'):
            return {'error': {'description': 'Unknown operation {}'.format(action)}}, 400

        if action == 'start':
            if AnnotatorContainer.is_running_for(collection_id):
                return {'error': {'description': 'Annotator already running (collection: {})'.format(collection_id)}}, 400
            AnnotatorContainer.launch_in_background(collection_id)
        elif action == 'stop':
            AnnotatorContainer.stop(collection_id)

        return {'result': 'success', 'action_performed': action}, 201


class RunningAnnotatorsApi(Resource):
    """
    API for `/running` endpoint
    """

    @marshal_with_field(fields.List(fields.Raw))
    def get(self):
        return dict(AnnotatorContainer.running()), 200


class AnnotatorModels(Resource):
    """
    API for `/models` endpoint
    """

    @marshal_with_field(fields.Raw)
    def get(self):
        return AnnotatorContainer.available_models(), 200


class AnnotatorCounters(Resource):
    """
    API for `/counters` endpoint
    """

    @marshal_with_field(fields.Raw)
    def get(self):
        return AnnotatorContainer.counters(), 200


if __name__ == 'start':

    def log_config():
        logger.info('CNN Models folder %s', models_path)
        logger.info('Loaded models')
        for lang, model in models.items():
            logger.info('%s --> %s', lang, model)

    api.add_resource(AnnotatorApi, '/<int:collection_id>/<string:action>')
    api.add_resource(RunningAnnotatorsApi, '/running')
    api.add_resource(AnnotatorModels, '/models')
    api.add_resource(AnnotatorCounters, '/counters')

    logger.info('[OK] Annotator Microservice ready for incoming requests')
    log_config()

    # start topic consumers for pipeline
    for language in available_languages:
        # if running docker compose on a single server/development,
        # we just bootstrap Annotator EN to avoid eating the whole memory
        if (DEVELOPMENT and language == 'en') or not DEVELOPMENT:
            logger.info('----- Starting KAFKA consumer on topic: annotator_%s', language)
            AnnotatorContainer.consumer_in_background(language)
