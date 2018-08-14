import os
import logging


from flask_restful import Resource, Api, fields, marshal_with, marshal_with_field

from smfrcore.models.sqlmodels import create_app
from smfrcore.utils import LOGGER_FORMAT, LOGGER_DATE_FORMAT

from geocoder import Geocoder


app = create_app()
api = Api(app)

logging.basicConfig(level=os.environ.get('LOGGING_LEVEL', 'DEBUG'), format=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)


logging.getLogger('cassandra').setLevel(logging.WARNING)
logging.getLogger('kafka').setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


class GeocoderApi(Resource):
    """
    Flask Restful API for Geocoder microservice
    """

    @marshal_with(
        {'error': fields.Nested({'description': fields.Raw}),
         'result': fields.Raw, 'action_performed': fields.Raw}
    )
    def put(self, collection_id, action):
        action = action.lower()
        if action not in ('start', 'stop'):
            return {'error': {'description': 'Unknown operation {}'.format(action)}}, 400

        if action == 'start':
            if Geocoder.is_running_for(collection_id):
                return {'error': {'description': 'Geocoder already running for {}'.format(collection_id)}}, 400
            Geocoder.launch_in_background(collection_id)
        elif action == 'stop':
            Geocoder.stop(collection_id)

        return {'result': 'success', 'action_performed': action}, 201


class RunningGeotaggersApi(Resource):
    """
    Flask Restful API for Geocoder microservice for `/running` endpoint
    """

    @marshal_with_field(fields.List(fields.Integer))
    def get(self):
        return Geocoder.running(), 200


api.add_resource(GeocoderApi, '/<int:collection_id>/<string:action>')
api.add_resource(RunningGeotaggersApi, '/running')
logger.info('Geocoder Microservice ready for incoming requests')
Geocoder.consumer_in_background()
