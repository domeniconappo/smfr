import logging

from flask import Flask
from flask_restful import Resource, Api

from annotator import Annotator


app = Flask(__name__)
api = Api(app)


class AnnotatorApi(Resource):

    logger = logging.getLogger(__name__)

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

        return {}, 204


class RunningAnnotatorsApi(Resource):

    logger = logging.getLogger(__name__)

    def get(self):
        return Annotator.running, 200


api.add_resource(AnnotatorApi, '/<int:collection_id>/<string:lang>/<string:action>')
api.add_resource(RunningAnnotatorsApi, '/running')
